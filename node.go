/*
Network node for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run [server ip:port]
*/

package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"./dkvlib"
)

////////////////////////////////////////////////////////////////////////////////
// ERRORS
////////////////////////////////////////////////////////////////////////////////

// Contains key
type InvalidKeyError string

func (e InvalidKeyError) Error() string {
	return fmt.Sprintf("Node: Invalid key [%s]", string(e))
}

// Contains serverAddr
type DisconnectedServerError string

func (e DisconnectedServerError) Error() string {
	return fmt.Sprintf("Node: Cannot connect to server [%s]", string(e))
}

type InvalidFailureError string

func (e InvalidFailureError) Error() string {
	return fmt.Sprintf("Server: Failure Alert invalid. Ignoring. [%s]", string(e))
}

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
	LocalAddr         net.Addr
	ServerAddr        string
	Server            *rpc.Client
	Coordinator       *Node
	allNodes          AllNodes = AllNodes{nodes: make(map[string]*Node)}
	isCoordinator     bool
	Settings          NodeSettings
	ID                string
	coordinatorFailed bool    = false
	kvstore           KVStore = KVStore{store: make(map[string]string)}
)

// For coordinator
var (
	allFailures AllFailures = AllFailures{nodes: make(map[string]*FailedNode)}
	voteTimeout int64       = int64(time.Millisecond * 20000)
)

////////////////////////////////////////////////////////////////////////////////
// TYPES, STRUCTURES
////////////////////////////////////////////////////////////////////////////////
type KVStore struct {
	sync.RWMutex
	store map[string]string
}

// Registration package
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

// Node Settings
type Node struct {
	ID              string
	IsCoordinator   bool
	Address         net.Addr
	RecentHeartbeat int64
	NodeConn        *rpc.Client
}

// Node Settings Known By Server
type SmallNode struct {
	ID            string
	IsCoordinator bool
	Address       net.Addr
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
	timestamp int64
	address   net.Addr
	reporters map[string]bool
}

type NodeInfo struct {
	ID      string
	Address net.Addr
}

type FailureInfo struct {
	Failed   net.Addr
	Reporter net.Addr
}

type CoordinatorFailureInfo struct {
	Failed         net.Addr
	Reporter       net.Addr
	NewCoordinator net.Addr
}

// For RPC Calls
type KVNode int

type ReadRequest struct {
	Key string
}

type ReadReply struct {
	Value   string
	Success bool
}

type WriteRequest struct {
	Key   string
	Value string
}

type DeleteRequest struct {
	Key string
}

type OpReply struct {
	Success bool
}

////////////////////////////////////////////////////////////////////////////////
// SERVER <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
// Connects to the server to join or initiate the KV network
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
	if err != nil {
		outLog.Printf("Failed to get a local addr:%s\n", err)
		return
	}
	LocalAddr = ln.Addr()

	// Connect to server
	outLog.Printf("Connect to server at %s...", serverAddr)
	Server, err = rpc.Dial("tcp", serverAddr)
	if err != nil {
		errLog.Printf("Failed to connected to server at %s", serverAddr)
		return
	}
	outLog.Printf("Successfully connected to server at %s!", serverAddr)
	outLog.Printf("My local address is %s", LocalAddr.String())

	// Register node to server
	err = RegisterNode()
	if err != nil {
		errLog.Println("Failed to register node")
		return
	}
	outLog.Println("Successfully registered node")

	// Connect to existing nodes
	GetNodes()
	outLog.Println("Connected to all existing nodes")

	// Save address to reconnect
	ServerAddr = serverAddr

	// Close server connection
	outLog.Printf("Closing connection to server...")
	err = Server.Close()
	if err != nil {
		outLog.Println("Server connection already closing:", err)
	} else {
		outLog.Printf("Server connection successfully closed! Node is ready!")
	}

	// Coordinator reports for duty to server
	if isCoordinator {
		ReportForCoordinatorDuty(serverAddr)
	}

	// Listen for other incoming nodes
	kvNode := new(KVNode)
	node := rpc.NewServer()
	node.Register(kvNode)

	for {
		conn, _ := ln.Accept()
		go node.ServeConn(conn)
	}
}

// Registers a node to the server and receives a node ID
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

// Retrieves all nodes existing in network
func GetNodes() (err error) {
	var nodeSet map[string]*Node

	err = Server.Call("KVServer.GetAllNodes", 0, &nodeSet)
	if err != nil {
		outLog.Println("Error getting existing nodes from server")
	} else {
		outLog.Println("Connecting to the other nodes...")
		for id, node := range nodeSet {
			if node.Address.String() != LocalAddr.String() {
				node.ID = id
				ConnectNode(node)
			}
		}
	}

	// Add self to allNodes map
	AddSelfToMap()

	// Ask coordinator for values in kvstore
	if !isCoordinator {
		GetValuesFromCoordinator()
	}

	return nil
}

// Add self to allNodes map
func AddSelfToMap() {
	selfNode := &Node{
		ID:            ID,
		IsCoordinator: isCoordinator,
		Address:       LocalAddr,
	}
	allNodes.Lock()
	allNodes.nodes[LocalAddr.String()] = selfNode
	allNodes.Unlock()
}

// Report node failure to coordinator
func ReportNodeFailure(node *Node) {
	info := &FailureInfo{
		Failed:   node.Address,
		Reporter: LocalAddr,
	}
	var reply int
	err := Coordinator.NodeConn.Call("KVNode.ReportNodeFailure", &info, &reply)
	if err != nil {
		outLog.Println("Error reporting failure of node ", node.Address)
	}
}

func ReportCoordinatorFailure(node *Node) {
	// If connection with server has failed, reconnect
	Server, err := rpc.Dial("tcp", ServerAddr)
	if err != nil {
		outLog.Println("Failed to reconnect with server.")
		// TODO: what do we do if we can't communicate with the server?
		return
	}

	outLog.Println("Reconnected with server to vote for failure of coordinator.")

	vote := voteNewCoordinator()

	allNodes.RLock()
	if _, ok := allNodes.nodes[node.Address.String()]; !ok {
		outLog.Println("Node does not exist.  Aborting coordinator failure alert.")
		allNodes.RUnlock()
		return
	}
	allNodes.RUnlock()

	info := &CoordinatorFailureInfo{
		Failed:         node.Address,
		Reporter:       LocalAddr,
		NewCoordinator: vote,
	}

	var reply int
	err = Server.Call("KVServer.ReportCoordinatorFailure", &info, &reply)
	if err != nil {
		outLog.Println("Error reporting failure of coordinator ", node.Address)
	} else {
		coordinatorFailed = true
	}
}

// Save the new coordinator
func (n KVNode) NewCoordinator(args *NodeInfo, _unused *int) (err error) {
	addr := args.Address
	outLog.Println("Received new coordinator... ", addr)

	allNodes.Lock()
	delete(allNodes.nodes, Coordinator.Address.String())

	if addr.String() == LocalAddr.String() {
		outLog.Println("I am the new coordinator!")
		allNodes.nodes[addr.String()].IsCoordinator = true
		isCoordinator = true
		ReportForCoordinatorDuty(ServerAddr)
	} else {
		if _, ok := allNodes.nodes[addr.String()]; ok {
			fmt.Printf("Node %s exists\n", addr.String())
		}
		allNodes.nodes[addr.String()].IsCoordinator = true
		Coordinator = allNodes.nodes[addr.String()]
		outLog.Println(addr, " is the new coordinator.")
	}
	allNodes.Unlock()

	// election complete, new coordinator elected
	coordinatorFailed = false
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Check for heartbeat timeouts from other nodes
func MonitorHeartBeats(addr string) {
	for {
		time.Sleep(time.Duration(Settings.HeartBeat+1000) * time.Millisecond)
		if _, ok := allNodes.nodes[addr]; ok {

			if time.Now().UnixNano()-allNodes.nodes[addr].RecentHeartbeat > int64(Settings.HeartBeat)*int64(time.Millisecond) {
				allNodes.RLock()
				if isCoordinator {
					SaveNodeFailure(allNodes.nodes[addr])
				} else if allNodes.nodes[addr].IsCoordinator && coordinatorFailed == false {
					ReportCoordinatorFailure(allNodes.nodes[addr])
				} else {
					ReportNodeFailure(allNodes.nodes[addr])
				}
				allNodes.RUnlock()
			}
		} else {
			outLog.Println("Node not found in network. Stop monitoring heartbeats.", addr)
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

// Request KVstore values from the coordinator
func GetValuesFromCoordinator() {
	var unused int
	var reply map[string]string

	outLog.Println("Requesting map values from coordinator...")

	err := Coordinator.NodeConn.Call("KVNode.RequestValues", unused, &reply)
	if err != nil {
		outLog.Printf("Could not retrieve kvstore values from coordinator: %s\n", err)
	}

	kvstore.Lock()
	kvstore.store = reply
	kvstore.Unlock()

	kvstore.RLock()
	outLog.Printf("This is the map:%v\n", kvstore.store)
	kvstore.RUnlock()
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Return kvstore to requesting node
func (n KVNode) RequestValues(unused int, reply *map[string]string) error {
	if !isCoordinator {
		handleErrorFatal("Network node attempting to run coordinator node function.", nil)
	}
	outLog.Println("Coordinator retrieving majority values for new node...")
	kvstore.RLock()
	*reply = kvstore.store
	kvstore.RUnlock()
	return nil
}

// Report to server as coordinator
func ReportForCoordinatorDuty(serverAddr string) {
	// Connect to server
	outLog.Printf("Coordinator connecting to server at %s...", serverAddr)
	conn, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		errLog.Printf("Coordinator failed to connected to server at %s", serverAddr)
		return
	}
	outLog.Printf("Coordinator successfully connected to server at %s!", serverAddr)
	Server = conn

	// Periodically update server about online nodes
	go UpdateOnlineNodes()
}

// Report connected nodes to server
func UpdateOnlineNodes() {
	for {
		var unused int

		allNodes.RLock()
		nodes := allNodes.nodes
		allNodes.RUnlock()

		nodeMap := make(map[string]*SmallNode)
		for k, v := range nodes {
			nodeMap[k] = &SmallNode{
				ID:            v.ID,
				IsCoordinator: v.IsCoordinator,
				Address:       v.Address,
			}
		}

		err := Server.Call("KVServer.GetOnlineNodes", nodeMap, &unused)
		if err != nil {
			outLog.Printf("Could not send online nodes to server: %s\n", err)
		} else {
			//outLog.Println("Server's map of online nodes updated...")
		}
		time.Sleep(5 * time.Second)
	}
}

// Coordinator has observed failure of a node
func SaveNodeFailure(node *Node) {
	addr := node.Address

	if !isCoordinator {
		handleErrorFatal("Network node attempting to run coordinator node function.", nil)
	}
	allFailures.Lock()
	if node, ok := allFailures.nodes[addr.String()]; ok {
		node.reporters[LocalAddr.String()] = true
		allFailures.Unlock()
	} else {
		reporters := make(map[string]bool)
		reporters[LocalAddr.String()] = true
		allFailures.nodes[addr.String()] = &FailedNode{
			timestamp: time.Now().UnixNano(),
			address:   addr,
			reporters: reporters,
		}
		allFailures.Unlock()
		go DetectFailure(addr, allFailures.nodes[addr.String()].timestamp)
	}

}

// Node failure report from network node
func (n KVNode) ReportNodeFailure(info *FailureInfo, _unused *int) error {
	failure := info.Failed
	reporter := info.Reporter

	outLog.Println("Failed node ", failure, " detected by ", reporter)

	allFailures.Lock()
	if node, ok := allFailures.nodes[failure.String()]; ok {
		if _, ok := node.reporters[reporter.String()]; !ok {
			node.reporters[reporter.String()] = true
		}
		outLog.Println(len(node.reporters), "votes received for ", failure)
		allFailures.Unlock()
	} else {
		// Check that this node actually exists
		allNodes.RLock()
		if _, ok := allNodes.nodes[failure.String()]; !ok {
			outLog.Println("Node does not exist in network.  Ignoring failure report.")
			return InvalidFailureError(failure.String())
		}

		// first detection of failure
		reporters := make(map[string]bool)
		reporters[reporter.String()] = true
		outLog.Println("First failure of ", failure)

		allFailures.nodes[failure.String()] = &FailedNode{
			timestamp: time.Now().UnixNano(),
			address:   failure,
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
	for time.Now().UnixNano() < timestamp+voteTimeout { //TODO: put timeout in config file
		allFailures.RLock()
		if len(allFailures.nodes[failureAddr.String()].reporters) >= quorum {
			outLog.Println("Quorum votes on failure reached for ", failureAddr.String())
			allFailures.RUnlock()

			// Remove from pending failures
			allFailures.Lock()
			if _, ok := allFailures.nodes[failureAddr.String()]; ok {
				delete(allFailures.nodes, failureAddr.String())
			}
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
func RemoveNode(node net.Addr) {
	outLog.Println("Removing ", node)
	allNodes.Lock()
	if _, ok := allNodes.nodes[node.String()]; ok {
		delete(allNodes.nodes, node.String())
	}
	allNodes.Unlock()

	allNodes.RLock()
	defer allNodes.RUnlock()

	// send broadcast to all network nodes declaring node failure
	var reply int
	args := &NodeInfo{
		Address: node,
	}
	for _, n := range allNodes.nodes {
		if n.Address.String() == LocalAddr.String() {
			// Do not send notice to self
			continue
		}
		err := n.NodeConn.Call("KVNode.NodeFailureAlert", &args, &reply)
		if err != nil {
			outLog.Println("Failure broadcast failed to ", n.Address)
		}
	}

	// send failure acknowledgement to server
	err := Server.Call("KVServer.NodeFailureAlert", &args, &reply)
	if err != nil {
		outLog.Println("Failure alert to server failed", err)
	}
}

// Returns quorum: num nodes / 2 + 1 or 1 if num nodes == 2
func getQuorumNum() int {
	if !isCoordinator {
		handleErrorFatal("Not a network node function.", nil)
	}
	allNodes.RLock()
	defer allNodes.RUnlock()
	size := len(allNodes.nodes)/2 + 1
	if size <= 2 {
		return 1
	}
	return size
}

// Vote for who they think should be the new coordinator
func voteNewCoordinator() net.Addr {
	allNodes.RLock()
	defer allNodes.RUnlock()

	lowestID, err := strconv.Atoi(ID) // get current node's id
	if err != nil {
		outLog.Println("Error retrieving local id.")
	}
	vote := LocalAddr

	// Look for the node with the lowest ID
	for _, node := range allNodes.nodes {
		if node.IsCoordinator {
			continue // Do not vote for current coordinator
		}
		id, err := strconv.Atoi(node.ID)
		if err != nil {
			outLog.Println("Error retreiving node ID. ", node.Address, " ID: ", node.ID)
		}

		if id < lowestID {
			lowestID = id
			vote = node.Address
		}
	}

	outLog.Println("Voting for ", vote, " as new coordinator.")
	return vote
}

func (n KVNode) SendHeartbeat(unused_args *int, reply *int64) error {
	//outLog.Println("Heartbeat request received from client.")
	*reply = time.Now().UnixNano()
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR NODE <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Read request to coordinator
func (n KVNode) CoordinatorRead(args ReadRequest, reply *ReadReply) error {
	outLog.Println("Coordinator received read operation")

	// ask all nodes for their values (vote)
	// map of values to their count
	valuesMap := make(map[string]int)
	quorum := getQuorumNum()

	// add own value to map if it exists
	kvstore.RLock()
	_, ok := kvstore.store[args.Key]
	if ok {
		addToValuesMap(valuesMap, kvstore.store[args.Key])
	}
	kvstore.RUnlock()

	allNodes.Lock()
	outLog.Println("Attempting to read back-up nodes...")
	for _, node := range allNodes.nodes {
		if !node.IsCoordinator {
			outLog.Printf("Reading from node %s...\n", node.ID)

			nodeArgs := args
			nodeReply := ReadReply{}

			err := node.NodeConn.Call("KVNode.NodeRead", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not reach node ", node.ID)
			}

			// Record successes
			if !nodeReply.Success {
				outLog.Printf("Failed to read from node %s... \n", node.ID)
			} else {
				outLog.Printf("Successfully read from node %s...\n", node.ID)
				addToValuesMap(valuesMap, nodeReply.Value)
			}
		}

	}

	allNodes.Unlock()

	// Find value with majority and set in local kvstore
	// If no majority value exists, return success - false
	var results []string
	largestCount := 0
	for val, count := range valuesMap {
		if count > largestCount {
			largestCount = count
			results = nil
			results = append(results, val)
		} else if count == largestCount {
			results = append(results, val)
		}
	}
	outLog.Printf("LENGTH: %d\n", len(results))

	if largestCount >= quorum {
		var result string
		if len(results) == 1 {
			result = results[0]
		} else {
			result = results[rand.Intn(len(results)-1)]
		}
		// update coordinator kvstore
		kvstore.Lock()
		kvstore.store[args.Key] = result
		kvstore.Unlock()
		outLog.Println("Sending Read decision to back-up nodes")
		sendWriteToNodes(args.Key, result)
		reply.Value = result
		reply.Success = true

	} else {
		// update coordinator kvstore
		kvstore.Lock()
		delete(kvstore.store, args.Key)
		kvstore.Unlock()
		outLog.Println("Sending Delete to back-up nodes")
		sendDeleteToNodes(args.Key)
		reply.Value = ""
		reply.Success = false
	}

	return nil
}

func sendWriteToNodes(key string, value string) {
	allNodes.Lock()
	defer allNodes.Unlock()

	for _, node := range allNodes.nodes {
		if !node.IsCoordinator {
			outLog.Printf("Writing to node %s...\n", node.ID)

			nodeArgs := WriteRequest{
				Key:   key,
				Value: value,
			}
			nodeReply := OpReply{}

			err := node.NodeConn.Call("KVNode.NodeWrite", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not write to node ", err)
			}

			// Record successes
			if nodeReply.Success {
				outLog.Printf("Successfully wrote to node %s!\n", node.ID)
			} else {
				outLog.Printf("Failed to write to node %s...\n", node.ID)
			}
		}
	}
}

func sendDeleteToNodes(key string) {
	allNodes.Lock()
	defer allNodes.Unlock()

	// Attempt delete from backup nodes
	for _, node := range allNodes.nodes {
		if !node.IsCoordinator {
			outLog.Printf("Deleting from node %s...\n", node.ID)

			nodeArgs := DeleteRequest{
				Key: key,
			}
			nodeReply := OpReply{}

			err := node.NodeConn.Call("KVNode.NodeDelete", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not delete from node ", err)
			}

			// Record successes
			if nodeReply.Success {
				outLog.Printf("Successfully deleted from node %s!\n", node.ID)
			} else {
				outLog.Printf("Failed to delete from node %s...\n", node.ID)
			}
		}

	}

}

func addToValuesMap(valuesMap map[string]int, val string) {
	_, ok := valuesMap[val]
	if ok {
		valuesMap[val] = valuesMap[val] + 1
	} else {
		valuesMap[val] = 1
	}
}

func (n KVNode) NodeRead(args ReadRequest, reply *ReadReply) error {
	outLog.Println("Node received Read operation")
	kvstore.RLock()
	val, ok := kvstore.store[args.Key]

	kvstore.RUnlock()
	if !ok {
		reply.Success = false
		reply.Value = ""
	} else {
		reply.Success = true
		reply.Value = val
	}
	return nil
}

// Writing a KV pair to the coordinator node
func (n KVNode) CoordinatorWrite(args WriteRequest, reply *OpReply) error {
	allNodes.Lock()
	defer allNodes.Unlock()

	// Attempt to write to backup nodes
	successes := 0
	outLog.Println("Attempting to write to back-up nodes...")
	for _, node := range allNodes.nodes {
		if !node.IsCoordinator {
			outLog.Printf("Writing to node %s...\n", node.ID)

			nodeArgs := args
			nodeReply := OpReply{}

			err := node.NodeConn.Call("KVNode.NodeWrite", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not write to node ", err)
			}

			// Record successes
			if nodeReply.Success {
				successes++
				outLog.Printf("Successfully wrote to node %s!\n", node.ID)
			} else {
				outLog.Printf("Failed to write to node %s...\n", node.ID)
			}
		}
	}

	// Check if majority of writes suceeded
	threshold := Settings.MajorityThreshold
	successRatio := float32(successes) / float32(len(allNodes.nodes))
	outLog.Println("This is the write success ratio:", successRatio)

	// Update coordinator
	if successRatio >= threshold || len(allNodes.nodes) == 1 {
		outLog.Println("Back up is successful! Updating coordinator KV store...")
		kvstore.Lock()
		defer kvstore.Unlock()

		key := args.Key
		value := args.Value
		kvstore.store[key] = value
		outLog.Printf("(%s, %s) successfully written to the KV store!\n", key, kvstore.store[key])
		b, err := json.MarshalIndent(kvstore.store, "", "  ")
		if err != nil {
			fmt.Println("error:", err)
		}
		fmt.Print(string(b))

		*reply = OpReply{Success: true}
	} else {
		outLog.Println("Back up failed! Aborting write...")
		*reply = OpReply{Success: false}
	}

	return nil
}

// Writing a KV pair to the network nodes
func (n KVNode) NodeWrite(args WriteRequest, reply *OpReply) error {
	outLog.Println("Received write request from coordinator!")
	key := args.Key
	value := args.Value
	kvstore.Lock()
	defer kvstore.Unlock()

	kvstore.store[key] = value
	outLog.Printf("(%s, %s) successfully written to the KV store!\n", key, kvstore.store[key])
	b, err := json.MarshalIndent(kvstore.store, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))

	*reply = OpReply{Success: true}
	*reply = OpReply{Success: true}

	return nil
}

// Deleting a KV pair from the coordinator node
func (n KVNode) CoordinatorDelete(args DeleteRequest, reply *OpReply) error {
	allNodes.Lock()
	defer allNodes.Unlock()

	// Attempt delete from backup nodes
	successes := 0
	outLog.Println("Attempting to delete from back-up nodes...")
	for _, node := range allNodes.nodes {
		if !node.IsCoordinator {
			outLog.Printf("Deleting from node %s...\n", node.ID)

			nodeArgs := args
			nodeReply := OpReply{}

			err := node.NodeConn.Call("KVNode.NodeDelete", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not delete from node ", err)
			}

			// Record successes
			if nodeReply.Success {
				successes++
				outLog.Printf("Successfully deleted from node %s!\n", node.ID)
			} else {
				outLog.Printf("Failed to delete from node %s...\n", node.ID)
			}
		}

	}

	// Check if majority of deletes suceeded
	threshold := Settings.MajorityThreshold
	successRatio := float32(successes) / float32(len(allNodes.nodes))
	outLog.Println("This is the delete success ratio:", successRatio)

	// Update coordinator
	if successRatio >= threshold || len(allNodes.nodes) == 1 {
		outLog.Println("Delete from back-up is successful! Updating coordinator KV store...")
		kvstore.Lock()
		defer kvstore.Unlock()

		key := args.Key
		value := kvstore.store[key]
		if _, ok := kvstore.store[key]; ok {
			delete(kvstore.store, key)
		} else {
			return dkvlib.NonexistentKeyError(key)
		}

		outLog.Printf("(%s, %s) successfully deleted from KV store!\n", key, value)

		*reply = OpReply{Success: true}
	} else {
		outLog.Println("Delete from network failed! Aborting delete...")
		*reply = OpReply{Success: false}
	}

	return nil
}

// Deleting a KV pair from the network nodes
func (n KVNode) NodeDelete(args DeleteRequest, reply *OpReply) error {
	outLog.Println("Received delete request from coordinator!")
	kvstore.Lock()
	defer kvstore.Unlock()

	key := args.Key
	value := kvstore.store[key]
	if _, ok := kvstore.store[key]; ok {
		delete(kvstore.store, key)
	} else {
		outLog.Printf("Key %s does not exist in store!\n", key)
		return dkvlib.NonexistentKeyError(key)
	}
	outLog.Printf("(%s, %s) successfully deleted from KV store!\n", key, value)

	*reply = OpReply{Success: true}

	return nil
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
		Coordinator = node
	}

	// Set up reverse connection
	args := &NodeInfo{
		Address: LocalAddr,
		ID:      ID,
	}
	var reply int
	err = nodeConn.Call("KVNode.RegisterNode", &args, &reply)
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
func (n KVNode) RegisterNode(args *NodeInfo, _unused *int) error {
	addr := args.Address
	id := args.ID

	//outLog.Println("Attempting to establish return connection")
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
	outLog.Println("Return connection with node succeeded: ", addr.String(), "ID: ", id)

	go sendHeartBeats(addr.String())

	go MonitorHeartBeats(addr.String())

	return nil
}

// send heartbeats to passed node
func sendHeartBeats(addr string) error {
	args := &NodeInfo{Address: LocalAddr, ID: ID}
	var reply int
	for {
		if _, ok := allNodes.nodes[addr]; !ok {
			outLog.Println("Node not found in network. Stop sending heartbeats", addr)
			return nil
		}
		err := allNodes.nodes[addr].NodeConn.Call("KVNode.ReceiveHeartBeats", &args, &reply)
		if err != nil {
			outLog.Println("Error sending heartbeats to", addr)

		}
		time.Sleep(time.Duration(Settings.HeartBeat) * time.Millisecond)
	}
}

// Log the most recent heartbeat received
func (n KVNode) ReceiveHeartBeats(args *NodeInfo, _unused *int) (err error) {
	addr := args.Address

	allNodes.Lock()
	defer allNodes.Unlock()

	if _, ok := allNodes.nodes[addr.String()]; !ok {
		return err
	}
	allNodes.nodes[addr.String()].RecentHeartbeat = time.Now().UnixNano()

	outLog.Println("Heartbeats received from Node ", args.ID)
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
