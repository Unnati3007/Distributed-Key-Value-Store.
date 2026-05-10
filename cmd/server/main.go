package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/Unnati3007/kv-store/internal/store"
)

func main() {
	id := flag.Uint("id", 1, "Node ID")
	addr := flag.String("addr", ":8001", "Listen address")
	peersStr := flag.String("peers", "", "Comma-separated peer addresses")
	flag.Parse()

	peers := []string{}
	if *peersStr != "" {
		peers = strings.Split(*peersStr, ",")
	}

	kv := store.New()
	log.Printf("Node %d starting on %s with %d peers", *id, *addr, len(peers))
	fmt.Printf("Store ready. Keys: %d\n", kv.Len())
	// Full gRPC server startup would be wired here
	_ = peers
	select {} // block forever
}
