/*
Network node for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run [server ip:port]
*/

package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
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
	errLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

// Variable related to the node
var (
	LocalAddr     net.Addr
	Server        *rpc.Client
	allNodes      AllNodes = AllNodes{nodes: make(map[string]*Node)}
	isCoordinator bool
	Settings      NodeSettings
	ID            string
)

////////////////////////////////////////////////////////////////////////////////
// TYPES, STRUCTURES
////////////////////////////////////////////////////////////////////////////////
// Registration package
type RegistrationPackage struct {
	Settings      NodeSettings
	ID            string
	IsCoordinator bool
}

// Node Settings
type NodeSettings struct {
	HeartBeat         uint32  `json:"heartbeat"`
	MajorityThreshold float32 `json:"majority-threshold"`
}

// Node Settings
type Node struct {
	ID              string
	IsCoordinator   bool
	Address         net.Addr
	RecentHeartbeat int64
	NodeConn        *rpc.Client
}

// All Nodes
type AllNodes struct {
	sync.RWMutex
	nodes map[string]*Node
}

type NodeInfo struct {
	Address net.Addr
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
	LocalAddr = ln.Addr()

	// Connect to server
	outLog.Printf("Connect to server at %s...", serverAddr)
	Server, err = rpc.Dial("tcp", serverAddr)
	if err != nil {
		errLog.Printf("Failed to connected to server at %s", serverAddr)
		return
	}
	outLog.Printf("Successfully connected to server at %s!", serverAddr)

	// Register node to server
	err = RegisterNode()
	if err != nil {
		errLog.Println("Failed to register node")
		return
	}
	outLog.Println("Successfully registered node")

	// Connect to existing nodes
	GetNodes()

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

func RegisterNode() (err error) {
	var regInfo RegistrationPackage

	nodeInfo := NodeInfo{Address: LocalAddr}
	err = Server.Call("KVServer.RegisterNode", nodeInfo, &regInfo)
	if err != nil {
		outLog.Println("Something bad happened:", err)
		return err
	}

	// Store node settings from server
	Settings = regInfo.Settings
	ID = regInfo.ID
	isCoordinator = regInfo.IsCoordinator

	if isCoordinator {
		outLog.Printf("Received node ID %s and this node is the coordinator!", ID)
	} else {
		outLog.Printf("Received node ID %s and this node is a network node", ID)
	}

	return nil
}

func GetNodes() (err error){
	var addrSet []net.Addr

	outLog.Println("Checking for existing nodes")
	err = Server.Call("KVServer.GetAllNodes", 0, &addrSet)
	if err != nil {
		outLog.Println("Error getting existing nodes from server")
	} else {
		for _, addr := range addrSet{
			outLog.Println(addr.String())
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
func ConnectToCoordinator() {
	return
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR FUNCTION
////////////////////////////////////////////////////////////////////////////////

func AddNodeToNetwork() {
	return
}

func CreatePrimaryBackup() {
	return
}

////////////////////////////////////////////////////////////////////////////////
// NODE <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
func sendHeartBeats() {
	return
}

////////////////////////////////////////////////////////////////////////////////
// MAIN, LOCAL
////////////////////////////////////////////////////////////////////////////////

func main() {
	gob.Register(&net.TCPAddr{})

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
