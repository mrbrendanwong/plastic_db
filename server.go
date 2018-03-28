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
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// ERRORS
////////////////////////////////////////////////////////////////////////////////

// ID has already been assigned in the server
type IDAlreadyRegisteredError string

func (e IDAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("Server: ID already registered [%s]", string(e))
}

// Address already registered in the server
type AddressAlreadyRegisteredError string

func (e AddressAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("Server: Address already registered [%s]", string(e))
}

// Misc. registration error
type RegistrationError string

func (e RegistrationError) Error() string {
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
	allNodes           AllNodes = AllNodes{Nodes: make(map[string]*Node)}
	currentCoordinator Node
	nextID             int = 0
)

// Variables for failures
var (
	voteInPlace bool        /* block communication with client when true */
	allFailures AllFailures = AllFailures{nodes: make(map[string]bool)}
	allVotes    AllVotes    = AllVotes{votes: make(map[string]int)}
	voteTimeout int64       = int64(time.Millisecond * 20000)
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
	HeartBeat            uint32  `json:"heartbeat"`
	VotingWait           uint32  `json:"voting-wait"`
	ElectionWait         uint32  `json:"election-wait"`
	ServerUpdateInterval uint32  `json:"server-update-interval"`
	MajorityThreshold    float32 `json:"majority-threshold"`
}

// Node - a node of the network
type Node SmallNode
type SmallNode struct {
	ID            string 
	IsCoordinator bool 
	Address       net.Addr 
}

// All Nodes - a map containing all nodes, including the coordinator
type AllNodes struct {
	sync.RWMutex
	Nodes map[string]*Node
}

type NodeInfo struct {
	ID      string
	Address net.Addr
}

type CoordinatorFailureInfo struct {
	Failed         net.Addr
	Reporter       net.Addr
	NewCoordinator net.Addr
}

type AllFailures struct {
	sync.RWMutex
	nodes map[string]bool
}

type AllVotes struct {
	sync.RWMutex
	votes map[string]int
}

type OnlineMap struct {
	Map map[string]*Node
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
	if _, exists := allNodes.Nodes[id]; exists {
		return IDAlreadyRegisteredError(id)
	}

	// Set node information and add to map
	allNodes.Nodes[id] = &Node{
		IsCoordinator: false,
		Address:       nodeInfo.Address,
	}
	// Check if this is the first node; if so set iscoordinator
	// and set current coordinator
	if len(allNodes.Nodes) == 1 {
		allNodes.Nodes[id].IsCoordinator = true
		// Set current coordinator
		currentCoordinator = *allNodes.Nodes[id]
	}

	// Reply
	*settings = RegistrationPackage{Settings: config.NodeSettings,
		ID:            id,
		IsCoordinator: allNodes.Nodes[id].IsCoordinator}

	outLog.Printf("Got register from %s\n", nodeInfo.Address.String())
	outLog.Printf("Gave node ID %s\n", id)

    outLog.Printf("allNodes:%v\n", allNodes.Nodes)
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
	*response = allNodes.Nodes
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

// Report failed network node
func (s KVServer) NodeFailureAlert(info *NodeInfo, _unused *int) error {
	failedNode := info.Address

	allNodes.Lock()
	defer allNodes.Unlock()

	delete(allNodes.Nodes, failedNode.String())
	outLog.Println("Node removed from system: ", failedNode.String())

	return nil
}

// Incoming coordinator failure report from network node
func (s KVServer) ReportCoordinatorFailure(info *CoordinatorFailureInfo, _unused *int) error {
	failed := info.Failed
	reporter := info.Reporter
	voted := info.NewCoordinator

	if len(allFailures.nodes) == 0 {
		voteInPlace = true
		outLog.Println("First reported failure of coordinator ", failed, " received from ", reporter)

		// First failure report, start listening for other reporting nodes
		allFailures.nodes[reporter.String()] = true

		// acknowledge vote
		castVote(voted.String())
		go DetectCoordinatorFailure(time.Now().UnixNano())

	} else {
		if _, ok := allFailures.nodes[reporter.String()]; !ok {
			outLog.Println("Reported failure of coordinator ", failed, " received from ", reporter)

			// if coordinator failure report has not yet been received by this reporter,
			// save report
			allFailures.nodes[reporter.String()] = true

			// save vote
			castVote(voted.String())
		}
	}

	return nil
}

// TODO: Called when server fails to receive heartbeats from coordinator
func CoordinatorConnectionFailure() {
	if len(allFailures.nodes) == 0 {
		voteInPlace = true
		outLog.Println("First report failures of coordinator.")

		allFailures.nodes[config.ServerAddress] = true
		go DetectCoordinatorFailure(time.Now().UnixNano())
	} else {
		if _, ok := allFailures.nodes[config.ServerAddress]; !ok {
			allFailures.nodes[config.ServerAddress] = true

			// TODO: vote network node with the lowest id to be new coordinator
		}
	}
}

// Listen for quorum number of failure reports
func DetectCoordinatorFailure(timestamp int64) {

	var didFail bool = false
	quorum := getQuorumNum()

	for time.Now().UnixNano() < timestamp+voteTimeout {
		allFailures.RLock()
		if len(allFailures.nodes) >= quorum {
			//quorum reached, coordinator failed
			didFail = true
			allFailures.RUnlock()
			break
		}
		allFailures.RUnlock()
	}
	if didFail {
		// tally up votes
		electedCoordinator := ElectCoordinator()
		// To stop the compilation errors
		fmt.Printf("Elected coordinator:%v\n", electedCoordinator)

		// Remove node from list of nodes
		allNodes.Lock()
		delete(allNodes.Nodes, currentCoordinator.Address.String())
		allNodes.Unlock()
		outLog.Println("Quorum reports of coordinator reached.")

		// TODO: broadcast new coordinator

	} else {
		// timeout, reports are invalid
		outLog.Println("Detecting coordinator failure timed out.  Failure reports invalid.")
	}

	// clear map of failures
	allFailures.nodes = make(map[string]bool)
	return
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR -> SERVER FUNCTIONS
////////////////////////////////////////////////////////////////////////////////

// Receive map of online nodes from coordinator
func (s *KVServer) GetOnlineNodes(args map[string]*Node, unused *int) (err error) {
	allNodes.Lock()
	allNodes.Nodes = args
	allNodes.Unlock()
	// if number of online nodes is 0, return empty as 1
    fmt.Printf("Server nodes:%v\n", args)
    for k,v := range args {
        fmt.Printf("k:%v, v:%v\n", k, v)
    }
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR ELECTION
////////////////////////////////////////////////////////////////////////////////

func ElectCoordinator() string {
	allVotes.RLock()
	defer allVotes.RUnlock()

	var maxVotes int
	var electedCoordinator string
	mostPopular := []string{}

	for node, numVotes := range allVotes.votes {
		if len(mostPopular) == 0 { // append first node of list
			mostPopular = append(mostPopular, node)
			maxVotes = numVotes
		} else if numVotes > maxVotes { // if current node has more votes than the ones seen before, replace entire list with this node
			mostPopular = nil
			mostPopular = append(mostPopular, node)
		} else if numVotes == maxVotes { // if current node has the max number of notes, add to list
			mostPopular = append(mostPopular, node)
		}
	}

	if len(mostPopular) > 1 {
		// if there is a tie, elect randomly
		rand.Seed(time.Now().UnixNano())
		index := rand.Intn(len(mostPopular) - 1)
		electedCoordinator = mostPopular[index]
		outLog.Println("Tie exists.  Randomly elected new coordinator: ", electedCoordinator)
		return electedCoordinator
	}

	electedCoordinator = mostPopular[0]
	outLog.Println("New coordinator elected: ", electedCoordinator)
	return electedCoordinator
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

func getQuorumNum() int {
	return len(allNodes.Nodes)/2 + 1
}

func castVote(addr string) {
	allVotes.Lock()
	defer allVotes.Unlock()

	if _, ok := allVotes.votes[addr]; ok {
		allVotes.votes[addr]++
	} else {
		allVotes.votes[addr] = 1
	}

	outLog.Println("Vote for ", addr, " casted.")
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
