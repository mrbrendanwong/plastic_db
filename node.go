/*
Network node for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run [server ip:port]
*/

package main

import (
	"os"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////
// ERRORS
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// TYPES, VARIABLES, CONSTANTS
////////////////////////////////////////////////////////////////////////////////

// Variables related to general node function
var (
	errLog		*log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog		*log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

// Variable related to the node
var (
	Server 			*rpc.Client

	allNodes 		AllNodes = AllNodes{nodes: make(map[string]*Node)}
	isCoordinator 	bool
)

////////////////////////////////////////////////////////////////////////////////
// TYPES, STRUCTURES
////////////////////////////////////////////////////////////////////////////////

// Node Settings
type NodeSettings struct {
	HeartBeat 			uint32 "json:heartbeat"
	MajorityThreshold 	float32 "json:majority-threshold"
}

// Node Settings
type Node struct {
	IsCoordinator 		bool
	Address				net.Addr
	RecentHeartbeat 	int64
	NodeConn 			*rpc.Client

}

// All Nodes
type AllNodes struct {
	sync.RWMutex
	nodes map[string]*Node
}

// For RPC Calls
type KVNode int

////////////////////////////////////////////////////////////////////////////////
// SERVER <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
func ConnectServer(serverAddr string) {
	// Look up local addr to use for this node
	var localAddr string
	localHostName, _ := os.Hostname()
	listOfAddr, _ := net.LookupIP(localHostName)
	for _, addr := range listOfAddr {
		if ok := addr.To4(); ok != nil {
			localAddr = ok.String()
		}
	}
	localAddr = fmt.Sprintf("%s%s", localAddr, ":0")
	ln, err := net.Listen("tcp", localAddr)

	// Connect to server
	outLog.Printf("Connect to server at %s...", serverAddr)
	Server, err = rpc.Dial("tcp", serverAddr)
	if err != nil {
		errLog.Printf("Failed to connected to server at %s", serverAddr)
		return
	}
	outLog.Printf("Successfully connected to server at %s!", serverAddr)

	// Register node to server

	// Store node settings from server

	// Listen for other incoming nodes
	kvNode := new(KVNode)
	node := rpc.NewServer()
	node.Register(kvNode)

	for {
		conn, _ := ln.Accept()
		go node.ServeConn(conn)
	}

	return
}

func RegisterNode(){
	return
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
func AddNodeToNetwork(){
	return
}

func ConnectToCoordinator(){
	return
}

func CreatePrimaryBackup(){
	return
}

////////////////////////////////////////////////////////////////////////////////
// NODE <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
func sendHeartBeats(){
	return
}

////////////////////////////////////////////////////////////////////////////////
// MAIN, LOCAL
////////////////////////////////////////////////////////////////////////////////

func main() {
	args := os.Args
	if len(args) != 2 {
		fmt.Println("Usage: go run node.go [server ip:port]")
		return
	}

	serverAddr := args[1]

	ConnectServer(serverAddr)

}

func handleErrorFatal(msg string, e error) {
	if e != nil {
		errLog.Fatalf("%s, err = %s\n", msg, e.Error())
	}
}