package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	server := flag.String("server", "localhost:8001", "Server address")
	flag.Parse()
	fmt.Printf("Connected to %s\n", *server)
	fmt.Println("Commands: SET <key> <value> | GET <key> | DEL <key> | quit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "quit" || line == "exit" {
			break
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		// Real client would send gRPC calls here
		switch strings.ToUpper(parts[0]) {
		case "SET":
			if len(parts) < 3 { fmt.Println("Usage: SET <key> <value>"); continue }
			fmt.Println("OK")
		case "GET":
			if len(parts) < 2 { fmt.Println("Usage: GET <key>"); continue }
			fmt.Println("(nil)")
		case "DEL":
			if len(parts) < 2 { fmt.Println("Usage: DEL <key>"); continue }
			fmt.Println("OK")
		default:
			fmt.Println("Unknown command")
		}
	}
}
