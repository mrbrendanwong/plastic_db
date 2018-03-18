package dkvlib

import (
	"errors"
	"log"
	"net/rpc"
	"os"
)

///////////////////////////////////////////////////////////////////////////
// ERROR DEFINITIONS
///////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////
// TYPES, VARIABLES, CONSTANTS
///////////////////////////////////////////////////////////////////////////

var (
	errLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

// Represent a Coordinator node
type CNodeConn interface {
	Read(key string) (string, error)
	Write(key, value string) error
	Update(key, value string) error
	Delete(key string) error
}

type CNode struct {
	coordinatorAddr string
	Coordinator     *rpc.Client
	connected       bool
}

func OpenCoordinatorConn(coordinatorAddr string) (cNodeConn CNodeConn, err error) {
	// Connect to coordinatorNode
	coordinator, err := rpc.Dial("tcp", coordinatorAddr)
	if err != nil {
		outLog.Println("Could not connect to coordinator.", errors.New("Coordinator Disconnected"))
		return nil, errors.New("Coordinator Disconnected")
	}

	// Create coord node
	cNodeConn = &CNode{coordinatorAddr, coordinator, true}

	return cNodeConn, nil
}

///////////////////////////////////////////////////////////////////////////
// CLIENT-COORDINATOR FUNCTIONS
///////////////////////////////////////////////////////////////////////////

// Get value of key
func (c CNode) Read(key string) (string, error) {
	// TODO
	return "", nil
}

// Write value to key
func (c CNode) Write(key, value string) error {
	outLog.Printf("WRITING KEY: %s with VALUE: %s\n", key, value)
	args := struct {
		Key   string
		Value string
	}{
		key,
		value,
	}
	var reply int
	outLog.Printf("Sending write to coordinator")
	err := c.Coordinator.Call("KVNode.Write", &args, &reply)
	if err != nil {
		outLog.Println("Could not connect to coordinator: ", err)
		return err
	}
	outLog.Printf("Successfully completed write to coordinator")
	return nil
}

// Update value of key
func (c CNode) Update(key, value string) error {
	//TODO
	return nil
}

// Delete key-value pair
func (c CNode) Delete(key string) error {
	//TODO
	return nil
}
