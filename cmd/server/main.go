package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"shardscale/internal/api"
	"shardscale/internal/metrics"
	"shardscale/internal/node"
	"shardscale/internal/router"
	"shardscale/internal/server"
)

const shutdownTimeout = 10 * time.Second

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

func main() {
	nodeID := flag.String("node-id", os.Getenv("NODE_ID"), "Node ID (default from NODE_ID env)")
	nodeAddr := flag.String("node-addr", os.Getenv("NODE_ADDR"), "Node address (default from NODE_ADDR env)")
	peersStr := flag.String("peers", os.Getenv("PEERS"), "Peers in format node2=addr2,node3=addr3")
	virtualNodesStr := flag.String("virtual-nodes", os.Getenv("VIRTUAL_NODES"), "Virtual nodes per physical node (default 50)")
	replicationFactorStr := flag.String("replication-factor", os.Getenv("REPLICATION_FACTOR"), "Replication factor (default 2)")
	heartbeatIntervalStr := flag.String("heartbeat-interval", os.Getenv("HEARTBEAT_INTERVAL"), "Heartbeat interval (default 2s)")
	heartbeatTimeoutStr := flag.String("heartbeat-timeout", os.Getenv("HEARTBEAT_TIMEOUT"), "Heartbeat timeout (default 6s)")
	flag.Parse()

	// Validate required config
	if *nodeID == "" || *nodeAddr == "" {
		logger.Error("NODE_ID and NODE_ADDR are required")
		os.Exit(1)
	}

	// Parse virtual nodes count
	virtualNodes := 50
	if *virtualNodesStr != "" {
		if parsed, err := strconv.Atoi(*virtualNodesStr); err == nil && parsed > 0 {
			virtualNodes = parsed
		}
	}

	replicationFactor := 2
	if *replicationFactorStr != "" {
		parsed, err := strconv.Atoi(*replicationFactorStr)
		if err != nil || parsed < 1 {
			logger.Error("invalid replication factor",
				slog.String("value", *replicationFactorStr),
			)
			os.Exit(1)
		}
		replicationFactor = parsed
	}

	// Parse heartbeat timings
	heartbeatInterval := 2 * time.Second
	if *heartbeatIntervalStr != "" {
		parsed, err := time.ParseDuration(*heartbeatIntervalStr)
		if err != nil || parsed <= 0 {
			logger.Error("invalid heartbeat interval",
				slog.String("value", *heartbeatIntervalStr),
			)
			os.Exit(1)
		}
		heartbeatInterval = parsed
	}

	heartbeatTimeout := 6 * time.Second
	if *heartbeatTimeoutStr != "" {
		parsed, err := time.ParseDuration(*heartbeatTimeoutStr)
		if err != nil || parsed <= 0 {
			logger.Error("invalid heartbeat timeout",
				slog.String("value", *heartbeatTimeoutStr),
			)
			os.Exit(1)
		}
		heartbeatTimeout = parsed
	}

	// Parse PEERS environment variable
	peers := make(map[string]string)
	if *peersStr != "" {
		for _, pair := range strings.Split(*peersStr, ",") {
			parts := strings.Split(pair, "=")
			if len(parts) != 2 {
				logger.Error("invalid peers format, expected node=addr pairs separated by comma",
					slog.String("peers", *peersStr),
				)
				os.Exit(1)
			}
			nodeKey := strings.TrimSpace(parts[0])
			addr := strings.TrimSpace(parts[1])
			peers[nodeKey] = addr
		}
	}

	// Add self to peers if not already present
	if _, ok := peers[*nodeID]; !ok {
		peers[*nodeID] = *nodeAddr
	}

	logger.Info("starting shardscale node",
		slog.String("node_id", *nodeID),
		slog.String("node_addr", *nodeAddr),
		slog.Any("peers", peers),
		slog.Int("virtual_nodes", virtualNodes),
		slog.Int("replication_factor", replicationFactor),
		slog.Duration("heartbeat_interval", heartbeatInterval),
		slog.Duration("heartbeat_timeout", heartbeatTimeout),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create node with hash ring
	metricCollector := metrics.New()
	metricCollector.SetReplicationFactor(replicationFactor)
	n := node.New(*nodeID, *nodeAddr, peers, virtualNodes, replicationFactor, heartbeatInterval, heartbeatTimeout, metricCollector, logger)

	// Create handlers
	handlers := api.NewHandlers(n.Router, n.Rebalancer, metricCollector, logger)

	// Create and start HTTP server
	httpServer := server.New(*nodeAddr, logger, handlers, metricCollector)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- httpServer.Run()
	}()

	go broadcastJoin(*nodeID, *nodeAddr, n.Router, logger)
	n.Membership.Start()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received", slog.String("signal_context", ctx.Err().Error()))
	case err := <-serverErr:
		if err != nil {
			logger.Error("server terminated unexpectedly", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	n.Membership.Stop()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err := <-serverErr; err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("server stopped with error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("server stopped cleanly")
}

func broadcastJoin(selfID, selfAddr string, r *router.Router, logger *slog.Logger) {
	peers := r.PeerSnapshot()
	payload, err := json.Marshal(map[string]string{
		"node_id":   selfID,
		"node_addr": selfAddr,
	})
	if err != nil {
		logger.Warn("failed to marshal join payload", slog.String("error", err.Error()))
		return
	}

	client := &http.Client{Timeout: 2 * time.Second}
	for nodeID, addr := range peers {
		if nodeID == selfID {
			continue
		}

		req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/internal/join", bytes.NewReader(payload))
		if err != nil {
			logger.Warn("failed creating join request",
				slog.String("target_node", nodeID),
				slog.String("error", err.Error()),
			)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			logger.Warn("join broadcast failed",
				slog.String("target_node", nodeID),
				slog.String("addr", addr),
				slog.String("error", err.Error()),
			)
			continue
		}
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			logger.Warn("join broadcast returned non-200",
				slog.String("target_node", nodeID),
				slog.Int("status", resp.StatusCode),
			)
		}
	}
}
