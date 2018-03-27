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
	kvstore       KVStore = KVStore{store: make(map[string]string)}
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

// All Nodes
type AllNodes struct {
	sync.RWMutex
	nodes map[string]*Node
}

type NodeInfo struct {
	ID      string
	Address net.Addr
}

// For RPC Calls
type KVNode int

type WriteRequest struct {
	Key   string
	Value string
}

type DeleteRequest struct {
	Key   string
}

type OpReply struct {
	Success 	bool
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

	// Register node to server
	err = RegisterNode()
	if err != nil {
		errLog.Println("Failed to register node")
		return
	}
	outLog.Println("Successfully registered node")

	// Connect to existing nodes
	GetNodes()

	// Close server connection
	outLog.Printf("Closing connection to server...")
	err = Server.Close()
	if err != nil {
		outLog.Println("Server connection already closing:", err)
	} else {
		outLog.Printf("Server connection successfully closed! Network node ready!")
	}

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

// Retreives all nodes existing in network
func GetNodes() (err error) {
	var nodeSet map[string]*Node

	err = Server.Call("KVServer.GetAllNodes", 0, &nodeSet)
	if err != nil {
		outLog.Println("Error getting existing nodes from server")
	} else {
		for _, node := range nodeSet {
			if node.Address.String() != LocalAddr.String() {
				ConnectNode(node)
			}
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Check for heartbeat timeouts from other nodes
func MonitorHeartBeats(addr string) {
	for {
		time.Sleep(time.Duration(Settings.HeartBeat+1000) * time.Millisecond)
		allNodes.RLock()
		if time.Now().UnixNano()-allNodes.nodes[addr].RecentHeartbeat > int64(Settings.HeartBeat)*int64(time.Millisecond) {
			if isCoordinator {
				outLog.Println("Connection with ", addr, " timed out.")
				//TODO: report coordinator - node failure
			} else if allNodes.nodes[addr].IsCoordinator {
				outLog.Println("Connection with coordinator timed out.")
				//TODO: handle coordinator failure
			} else {
				outLog.Println("Connection with ", addr, " timed out.")
				//TODO: handle node - node failure
			}
		}
		allNodes.RUnlock()
	}
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR FUNCTION // Is this section needed anymore?
////////////////////////////////////////////////////////////////////////////////
func (n KVNode) SendHeartbeat(unused_args *int, reply *int64) error {
	//outLog.Println("Heartbeat request received from client.")
	*reply = time.Now().UnixNano()
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR NODE <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
func (n KVNode) CoordinatorRead(key *string, value *string) error {
	// TODO ask all nodes for their values (vote)
	outLog.Println("Coordinator received read operation")
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

	// Check if majority of writes suceeded
	threshold := Settings.MajorityThreshold
	successRatio := float32(successes) / float32(len(allNodes.nodes))
	outLog.Println("This is the write success ratio:", successRatio)

	// Update coordinator
	if successRatio >= threshold {
		outLog.Println("Back up is successful! Updating coordinator KV store...")
		kvstore.Lock()
		defer kvstore.Unlock()

		key := args.Key
		value := args.Value
		kvstore.store[key] = value
		outLog.Printf("(%s, %s) successfully written to the KV store!\n", key, kvstore.store[key])

		*reply = OpReply{Success: true}
	} else {
		outLog.Println("Back up failed! Aborting write...")
		// TODO Roll back all writes on network nodes
		// Should we have a history structure?
		// thresholdString := strconv.Itoa(threshold)
		*reply = OpReply{Success: false}
	}

	return nil
}

// Writing a KV pair to the network nodes
func (n KVNode) NodeWrite(args WriteRequest, reply *OpReply) error {
	// TODO Keep current KVStore as history for rollback if needed
	outLog.Println("Received write request from coordinator!")
	key := args.Key
	value := args.Value
	kvstore.Lock()
	defer kvstore.Unlock()

	kvstore.store[key] = value
	outLog.Printf("(%s, %s) successfully written to the KV store!\n", key, kvstore.store[key])

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

	// Check if majority of deletes suceeded
	threshold := Settings.MajorityThreshold
	successRatio := float32(successes) / float32(len(allNodes.nodes))
	outLog.Println("This is the delete success ratio:", successRatio)

	// Update coordinator
	if successRatio >= threshold {
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
		// TODO Roll back all deletes on network nodes
		// Should we have a history structure?
		// thresholdString := strconv.Itoa(threshold)
		*reply = OpReply{Success: false}
	}

	return nil
}

// Deleting a KV pair from the network nodes
func (n KVNode) NodeDelete(args DeleteRequest, reply *OpReply) error {
	// TODO Keep current KVStore as history for rollback if needed
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
	defer allNodes.Unlock()
	allNodes.nodes[nodeAddr.String()] = node

	outLog.Println("Successfully connected to ", nodeAddr.String())

	// send heartbeats
	go sendHeartBeats(nodeConn)

	//TODO: check for timeouts
	go MonitorHeartBeats(nodeAddr.String())
	return nil
}

// Open reverse connection through RPC
func (n KVNode) RegisterNode(args *NodeInfo, _unused *int) error {
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
	defer allNodes.Unlock()

	allNodes.nodes[addr.String()] = &Node{
		id,
		false,
		addr,
		time.Now().UnixNano(),
		conn,
	}

	outLog.Println("Return connection with node succeeded: ", addr.String())

	go sendHeartBeats(conn)

	go MonitorHeartBeats(addr.String())

	return nil
}

// send heartbeats to passed node
func sendHeartBeats(conn *rpc.Client) error {
	args := &NodeInfo{Address: LocalAddr}
	var reply int
	for {
		err := conn.Call("KVNode.ReceiveHeartBeats", &args, &reply)
		if err != nil {
			//outLog.Println("Error sending heartbeats")
			//return err
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

	//outLog.Println("Heartbeats received by ", addr.String())
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
