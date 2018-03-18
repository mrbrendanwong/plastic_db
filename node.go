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
	"time"
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
	Coordinator	  *rpc.Client
	allNodes      AllNodes = AllNodes{nodes: make(map[string]*Node)}
	isCoordinator bool
	Settings      NodeSettings
	ID            string
)


// For coordinator
var (
	allFailures  	AllFailures = AllFailures{nodes: make(map[string]*FailedNode)}
	voteTimeout		int64 = int64(time.Millisecond * 20000)

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

type AllFailures struct {
	sync.RWMutex
	nodes map[string]*FailedNode
}

type FailedNode struct {
	timestamp	int64
	address 	net.Addr
	//votes		int
	reporters	map[string]bool
	//isFailure	chan bool
}

type NodeInfo struct {
	ID		string
	Address net.Addr
}

type FailureInfo struct {
	Failed		net.Addr
	Reporter	net.Addr
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

// Retreives all nodes existing in network
func GetNodes() (err error){
	var nodeSet map[string]*Node

	err = Server.Call("KVServer.GetAllNodes", 0, &nodeSet)
	if err != nil {
		outLog.Println("Error getting existing nodes from server")
	} else {
		for _, node := range nodeSet{
			if node.Address.String() != LocalAddr.String() {
				ConnectNode(node)
			}
		}
	}
	return nil
}

func ReportNodeFailure(node *Node){
	info := &FailureInfo{
		Failed: node.Address,
		Reporter: LocalAddr,
	}
	var reply int
	err := Coordinator.Call("KVNode.ReportNodeFailure", &info, &reply)
	if err != nil {
		outLog.Println("Error reporting failure of node ", node.Address)
	}
}

func ReportCoordinatorFailure(node *Node){
	// If connection with server has failed, reconnect

	info := &FailureInfo{
		Failed: node.Address,
		Reporter: LocalAddr,
	}

	var reply int
	err := Server.Call("KVServer.ReportCoordinatorFailure", &info, &reply)
	if err != nil {
		outLog.Println("Error reporting failure of coordinator ", node.Address)
	}
}



////////////////////////////////////////////////////////////////////////////////
// NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
func ConnectToCoordinator() {
	return
}

// Check for heartbeat timeouts from other nodes
func MonitorHeartBeats(addr string){
	for {
		time.Sleep(time.Duration(Settings.HeartBeat+1000) * time.Millisecond)
		if _, ok := allNodes.nodes[addr]; ok{

			if time.Now().UnixNano()-allNodes.nodes[addr].RecentHeartbeat > int64(Settings.HeartBeat)*int64(time.Millisecond) {
				allNodes.RLock()
				if (isCoordinator) {
					outLog.Println("Connection with ", addr, " timed out.")
					SaveNodeFailure(allNodes.nodes[addr])
				} else if (allNodes.nodes[addr].IsCoordinator) {
					outLog.Println("Connection with coordinator timed out.")
					ReportCoordinatorFailure(allNodes.nodes[addr])
				} else {
					outLog.Println("Connection with ", addr, " timed out.")
					ReportNodeFailure(allNodes.nodes[addr])
				}
				allNodes.RUnlock()
			}
		} else {
			outLog.Println("Node not found.", addr)
			return
		}

	}
}

// Broadcast of node failure from coordinator
func (n KVNode) NodeFailureAlert(node *NodeInfo, _unused *int) error {
	outLog.Println(" Node failure alert received from coordinator:  ", node.Address)

	allNodes.Lock()
	defer allNodes.Unlock()

	// remove node from list of nodes
	delete(allNodes.nodes, node.Address.String())
	outLog.Println(" Node successfully removed: ", node.Address)
	return nil
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

// Coordinator has observed failure of a node
func SaveNodeFailure(node *Node){
	addr := node.Address

	if !isCoordinator {
		handleErrorFatal("Network node attempting to run coordinator node function.", nil)
	}
	allFailures.Lock()
	if node, ok := allFailures.nodes[addr.String()]; ok{
		node.reporters[LocalAddr.String()] = true
		allFailures.Unlock()
	} else {
		reporters := make(map[string]bool)
		allFailures.nodes[addr.String()] = &FailedNode{
			timestamp: time.Now().UnixNano(),
			address: addr,
			reporters: reporters,
		}
		allFailures.Unlock()
		go DetectFailure(addr, allFailures.nodes[addr.String()].timestamp)
	}

}


// Node failure report from network node
func (n KVNode) ReportNodeFailure( info *FailureInfo, _unused *int ) error{
	failure := info.Failed
	reporter := info.Reporter

	outLog.Println("Failed node ", failure, " detected by ", reporter)

	allFailures.Lock()
	if node, ok := allFailures.nodes[failure.String()]; ok {				// TODO: do not increment vote if report from reporter node has already been received
		if _, ok := node.reporters[reporter.String()]; !ok{
			node.reporters[reporter.String()] = true
		}
		outLog.Println(len(node.reporters), "votes received for ", failure)
		allFailures.Unlock()
	} else {
		// first detection of failure
		reporters := make(map[string]bool)
		reporters[reporter.String()] = true
		outLog.Println("First failure of ", failure)

		allFailures.nodes[failure.String()] = &FailedNode{
			timestamp: time.Now().UnixNano(),
			address: failure,
			reporters: reporters,
		}

		allFailures.Unlock()

		go DetectFailure(failure, allFailures.nodes[failure.String()].timestamp)
	}

	return nil
}

// Begin listening for failure reports for given node
func DetectFailure(failureAddr net.Addr, timestamp int64) {
	quorum := getQuorumNum()

	// if time window has passed, and quorum not reached, failure is considered invalid
	for time.Now().UnixNano() < timestamp + voteTimeout {		//TODO: put timeout in config file
		allFailures.RLock()
		if len(allFailures.nodes[failureAddr.String()].reporters) >= quorum {
			outLog.Println("Quorum votes on failure reached for ", failureAddr.String())
			allFailures.RUnlock()

			// Remove from pending failures
			allFailures.Lock()
			delete(allFailures.nodes, failureAddr.String())
			allFailures.Unlock()

			RemoveNode(failureAddr)
			return
		}
		allFailures.RUnlock()
		time.Sleep(time.Millisecond)
	}

	// Remove from pending failures
	allFailures.Lock()
	delete(allFailures.nodes, failureAddr.String())
	allFailures.Unlock()
	// TODO: TELL NODES TO RECONNECT?
	outLog.Println("Timeout reached.  Failure invalid for ", failureAddr.String())
}

// Remove node from network
func RemoveNode(node net.Addr){
	outLog.Println("Removing ", node)
	allNodes.Lock()
	delete(allNodes.nodes, node.String())
	allNodes.Unlock()

	allNodes.RLock()
	defer allNodes.RUnlock()

	// send broadcast to all network nodes declaring node failure
	for _, n := range allNodes.nodes {
		var reply int
		args:= &NodeInfo{
			Address: node,
		}
		err := n.NodeConn.Call("KVNode.NodeFailureAlert", &args, &reply)
		if err != nil {
			outLog.Println("Failure broadcast failed to ", n.Address)
		}
	}

	//TODO: send failure acknowledgement to server
}


// Returns quorum: num nodes / 2 + 1
func getQuorumNum() int {
	if !isCoordinator {
		handleErrorFatal("Not a network node function.", nil)
	}
	allNodes.RLock()
	defer allNodes.RUnlock()
	return len(allNodes.nodes) / 2 + 1
}
////////////////////////////////////////////////////////////////////////////////
// NODE <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
// Connect to given node
func ConnectNode(node *Node) error {
	outLog.Println("Attempting to connected to node...", node.Address.String())
	nodeAddr := node.Address

	nodeConn, err := rpc.Dial("tcp", nodeAddr.String())
	if err != nil {
		outLog.Println("Could not reach node ", nodeAddr.String())
		return err
	}

	node.NodeConn = nodeConn

	// Save coordinator
	if node.IsCoordinator {
		Coordinator = nodeConn
	}

	// Set up reverse connection
	args := &NodeInfo{Address: LocalAddr}
	var reply int
	err = nodeConn.Call("KVNode.RegisterNode", args, &reply)
	if err != nil {
		outLog.Println("Could not initate connection with node: ", nodeAddr.String())
		return err
	}

	// Add this new node to node map
	allNodes.Lock()
	allNodes.nodes[nodeAddr.String()] = node
	allNodes.Unlock()

	outLog.Println("Successfully connected to ", nodeAddr.String())

	// send heartbeats
	go sendHeartBeats(nodeAddr.String())

	// check for timeouts
	go MonitorHeartBeats(nodeAddr.String())
	return nil
}

// Open reverse connection through RPC
func (n KVNode)RegisterNode(args *NodeInfo, _unused *int) error {
	addr := args.Address
	id := args.ID

	outLog.Println("Attempting to establish return connection")
	conn, err := rpc.Dial("tcp", addr.String())

	if err != nil {
		outLog.Println("Return connection with node failed: ", addr.String())
		return err
	}

	// Add node to node map
	allNodes.Lock()

	allNodes.nodes[addr.String()] = &Node{
		      id,
		      false,
		      addr,
		      time.Now().UnixNano(),
		      conn,
	}
	allNodes.Unlock()
	outLog.Println("Return connection with node succeeded: ", addr.String())

	go sendHeartBeats(addr.String())

	go MonitorHeartBeats(addr.String())

	return nil
}

// send heartbeats to passed node
func sendHeartBeats(addr string) error {
	args := &NodeInfo{Address: LocalAddr}
	var reply int
	for{
		if _,ok := allNodes.nodes[addr]; !ok {
			outLog.Println("Connection invalid.")
			return nil
		}
		err := allNodes.nodes[addr].NodeConn.Call("KVNode.ReceiveHeartBeats", &args, &reply)
		if err != nil {
			outLog.Println("Error sending heartbeats")

		}
		time.Sleep(time.Duration(Settings.HeartBeat)* time.Millisecond)
	}
}

// Log the most recent heartbeat received
func (n KVNode)ReceiveHeartBeats(args *NodeInfo, _unused *int) (err error) {
	addr := args.Address

	allNodes.Lock()
	defer allNodes.Unlock()

	if _, ok := allNodes.nodes[addr.String()]; !ok {
		return err
	}
	allNodes.nodes[addr.String()].RecentHeartbeat = time.Now().UnixNano()


	outLog.Println("Heartbeats received by ", addr.String())
	return nil
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
