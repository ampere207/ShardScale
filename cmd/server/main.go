package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"shardscale/internal/api"
	"shardscale/internal/metrics"
	"shardscale/internal/node"
	"shardscale/internal/server"
)

const shutdownTimeout = 10 * time.Second

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

func main() {
	nodeID := flag.String("node-id", os.Getenv("NODE_ID"), "Node ID (default from NODE_ID env)")
	nodeAddr := flag.String("node-addr", os.Getenv("NODE_ADDR"), "Node address (default from NODE_ADDR env)")
	peersStr := flag.String("peers", os.Getenv("PEERS"), "Peers in format node2=addr2,node3=addr3")
	virtualNodesStr := flag.String("virtual-nodes", os.Getenv("VIRTUAL_NODES"), "Virtual nodes per physical node (default 50)")
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
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create node with hash ring
	n := node.New(*nodeID, *nodeAddr, peers, virtualNodes, logger)

	// Create metrics and handlers
	metricCollector := metrics.New()
	handlers := api.NewHandlers(n.Router, metricCollector, logger)

	// Create and start HTTP server
	httpServer := server.New(*nodeAddr, logger, handlers, metricCollector)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- httpServer.Run()
	}()

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
