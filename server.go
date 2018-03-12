/*
Server for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run server.go
	-c Path to config.json file
*/

package main

import (
	"os"
	"flag"
	"io/ioutil"
	"math/rand"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
	"encoding/json"
)

////////////////////////////////////////////////////////////////////////////////
// ERRORS
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// TYPES, VARIABLES, CONSTANTS
////////////////////////////////////////////////////////////////////////////////

// Variables related to general server function
var  (
	config 		Config
	errLog		*log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog		*log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

// Variables related to nodes
var (
	allNodes 			AllNodes = AllNodes{nodes: make(map[string]*Node)}
	currentCoordinator 	Node
)
////////////////////////////////////////////////////////////////////////////////
// TYPES, STRUCTURES
////////////////////////////////////////////////////////////////////////////////

// Node Settings
type NodeSettings struct {
	HeartBeat 			uint32 "json:heartbeat"
	MajorityThreshold 	float32 "json:majority-threshold"
}

// Configuration
type Config struct {
	ServerAddress	string 			`json:"server-ip-port"`
	NodeSettings 	NodeSettings 	`json:"node-settings"`
}

// Node - a node of the network
type Node struct {
	IsCoordinator 	bool
	Address			net.Addr
}

// All Nodes - a map containing all nodes, including the coordinator
type AllNodes struct {
	sync.RWMutex
	nodes map[string]*Node
}

// For RPC calls
type KVServer int

////////////////////////////////////////////////////////////////////////////////
// SERVER <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Register a node into the KV node network
func (s *KVServer) RegisterNode() {
	return
}

// Elect a new coordinator node - elect a new coordinator based on the network majority vote
func (s *KVServer) ElectCoordinator() {
	return
}

// Get all nodes currently in the network
// *Useful if a heartbeat connection between miner dies, but the network is still online
func (s *KVServer) GetAllNodes() {
	return
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