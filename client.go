/*
Example client for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run client.go [server ip:port]
*/

package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"

	"./dkvlib"
)

var (
	errLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

var (
	server              *rpc.Client
	coordinator         *rpc.Client
	localAddr           net.Addr
	coordinatorNodeAddr net.Addr
	nodesAddrs          []net.Addr
)

// Node - a node of the network
type Node struct {
	ID            string
	IsCoordinator bool
	Address       net.Addr
}

// ConnectServer creates client connection with server
func ConnectServer(serverAddr string) {
	// Look up local addr for the client
	var localAddrStr string
	localHostName, _ := os.Hostname()
	listOfAddr, _ := net.LookupIP(localHostName)
	for _, addr := range listOfAddr {
		if ok := addr.To4(); ok != nil {
			localAddrStr = ok.String()
		}
	}
	localAddrStr = fmt.Sprintf("%s%s", localAddrStr, ":0")
	ln, err := net.Listen("tcp", localAddrStr)
	localAddr = ln.Addr()

	// Connect to server
	outLog.Printf("Connect to server at %s...", serverAddr)
	server, err = rpc.Dial("tcp", serverAddr)
	if err != nil {
		errLog.Printf("Failed to connected to server at %s", serverAddr)
		return
	}
	outLog.Printf("Successfully connected to server at %s!", serverAddr)
}

func main() {
	gob.Register(&net.TCPAddr{})

	if len(os.Args) != 2 {
		fmt.Println("Usage: ", os.Args[0], "server")
		os.Exit(1)
	}
	serverAddr := os.Args[1]

	// connect to server
	ConnectServer(serverAddr)
	var msg int
	var response net.Addr

	// Get coordinator node address
	err := server.Call("KVServer.GetCoordinatorAddress", msg, &response)
	if err != nil {
		outLog.Println("Couldn't retrieve coordinator address:", err)
	}
	coordinatorNodeAddr := response
	outLog.Printf("Coordinator address received: %v\n", coordinatorNodeAddr)

	// Connect to coordinator API

	coordinator, error := dkvlib.OpenCoordinatorConn(coordinatorNodeAddr.String())
	if error != nil {
		outLog.Println("Couldn't connect to dkvlib:", error)
	}
	// Call functions on API
	coordinator.Write("a", "5")

}
