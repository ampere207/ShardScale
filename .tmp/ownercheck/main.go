package main

import (
    "flag"
    "fmt"
    "strings"

    "shardscale/internal/ring"
)

func main() {
    key := flag.String("key", "", "key")
    nodesArg := flag.String("nodes", "node1,node2,node3", "comma-separated node ids")
    vn := flag.Int("vn", 50, "virtual nodes")
    rf := flag.Int("rf", 2, "replication factor")
    flag.Parse()

    nodes := []string{}
    for _, n := range strings.Split(*nodesArg, ",") {
        n = strings.TrimSpace(n)
        if n != "" {
            nodes = append(nodes, n)
        }
    }

    hr := ring.NewHashRing(*vn)
    hr.Rebuild(nodes)
    owners := hr.GetOwners(*key, *rf)

    fmt.Printf("key=%s owners=%v\n", *key, owners)
}
