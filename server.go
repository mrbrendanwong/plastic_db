/*
Server for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run server.go
	-c Path to config.json file
*/

package main

import (
	"encoding/gob"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
<<<<<<< HEAD
	"encoding/json"
	"encoding/gob"
	"strconv"
	"fmt"
=======
>>>>>>> dev
)

////////////////////////////////////////////////////////////////////////////////
// ERRORS
////////////////////////////////////////////////////////////////////////////////

// Contains ID
type IDAlreadyRegisteredError string

func (e IDAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("Server: ID already registered [%s]", string(e))
}


// Contains node address
type RegistrationError string

func(e RegistrationError) Error() string {
	return fmt.Sprintf("Server: Failure to register node [%s]", string(e))
}


////////////////////////////////////////////////////////////////////////////////
// TYPES, VARIABLES, CONSTANTS
////////////////////////////////////////////////////////////////////////////////

// Variables related to general server function
var (
	config Config
	errLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

// Variables related to nodes
var (
	allNodes           AllNodes = AllNodes{nodes: make(map[string]*Node)}
	currentCoordinator Node
	nextID             int = 0
)

////////////////////////////////////////////////////////////////////////////////
// TYPES, STRUCTURES
////////////////////////////////////////////////////////////////////////////////
// Configuration
type Config struct {
	ServerAddress string       `json:"server-ip-port"`
	NodeSettings  NodeSettings `json:"node-settings"`
}

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

// Node - a node of the network
type Node struct {
	ID            string
	IsCoordinator bool
	Address       net.Addr
}

// All Nodes - a map containing all nodes, including the coordinator
type AllNodes struct {
	sync.RWMutex
	nodes map[string]*Node
}

type NodeInfo struct {
	Address net.Addr
}

// For RPC calls
type KVServer int

////////////////////////////////////////////////////////////////////////////////
// SERVER <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Register a node into the KV node network
func (s *KVServer) RegisterNode(nodeInfo NodeInfo, settings *RegistrationPackage) error {
	allNodes.Lock()
	defer allNodes.Unlock()

	// Temporary method for assigning an ID
	id := strconv.Itoa(nextID)

	// Increment ID
	nextID++

	// Define errors
	if _, exists := allNodes.nodes[id]; exists {
		return IDAlreadyRegisteredError(id);
	}

	// Set node information and add to map
	allNodes.nodes[id] = &Node{
		IsCoordinator: false,
		Address:       nodeInfo.Address,
	}
	// Check if this is the first node; if so set iscoordinator
	// and set current coordinator
	if len(allNodes.nodes) == 1 {
		allNodes.nodes[id].IsCoordinator = true
		// Set current coordinator
		currentCoordinator = *allNodes.nodes[id]
	}

	// Reply
	*settings = RegistrationPackage{Settings: config.NodeSettings,
		ID:            id,
		IsCoordinator: allNodes.nodes[id].IsCoordinator}

	outLog.Printf("Got register from %s\n", nodeInfo.Address.String())

	return nil
}

// Elect a new coordinator node - elect a new coordinator based on the network majority vote
//func (s *KVServer) ElectCoordinator() error {
//	return nil
//}

// GetAllNodes currently in the network
// *Useful if a heartbeat connection between nodes dies, but the network is still online
func (s *KVServer) GetAllNodes(msg int, response *map[string]*Node) error {
	allNodes.RLock()
	*response = allNodes.nodes
	allNodes.RUnlock()
	return nil
}

// Set coordinator - set the first node of the network as a coordinator
func SetCoordinator() {
	return
}

// Send a Node to the coordinator to be set into the network
func SendToCoordinator() {
	return
}

////////////////////////////////////////////////////////////////////////////////
// SERVER <-> CLIENT FUNCTIONS
////////////////////////////////////////////////////////////////////////////////

// GetCoordinatorAddress sends coord address
func (s *KVServer) GetCoordinatorAddress(msg int, response *net.Addr) error {
	*response = currentCoordinator.Address
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// MAIN, LOCAL
////////////////////////////////////////////////////////////////////////////////

func readConfigOrDie(path string) {
	file, err := os.Open(path)
	handleErrorFatal("config file", err)

	buffer, err := ioutil.ReadAll(file)
	handleErrorFatal("read config", err)

	err = json.Unmarshal(buffer, &config)
	handleErrorFatal("parse config", err)
}

func main() {
	gob.Register(&net.TCPAddr{})

	path := flag.String("c", "", "Path to the JSON config")
	flag.Parse()

	if *path == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	readConfigOrDie(*path)

	rand.Seed(time.Now().UnixNano())

	kvserver := new(KVServer)

	server := rpc.NewServer()
	server.Register(kvserver)

	l, e := net.Listen("tcp", config.ServerAddress)

	handleErrorFatal("listen error", e)
	outLog.Printf("Server started. Receiving on %s\n", config.ServerAddress)

	for {
		conn, _ := l.Accept()
		go server.ServeConn(conn)
	}
}

func handleErrorFatal(msg string, e error) {
	if e != nil {
		errLog.Fatalf("%s, err = %s\n", msg, e.Error())
	}
}
