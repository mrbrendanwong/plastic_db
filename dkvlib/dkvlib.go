package dkvlib

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
)

///////////////////////////////////////////////////////////////////////////
// ERROR DEFINITIONS
///////////////////////////////////////////////////////////////////////////
type InvalidKeyCharError string

func (e InvalidKeyCharError) Error() string {
	return fmt.Sprintf("DKV: Invalid character in key [%s]", string(e))
}

type InvalidValueCharError string

func (e InvalidValueCharError) Error() string {
	return fmt.Sprintf("DKV: Invalid character in value [%s]", string(e))
}

type KeyTooLongError string

func (e KeyTooLongError) Error() string {
	return fmt.Sprintf("DKV: Key is above character limit [%s]", string(e))
}

type ValueTooLongError string

func (e ValueTooLongError) Error() string {
	return fmt.Sprintf("DKV: Value is above character limit [%s]", string(e))
}

type CoordinatorWriteError string

func (e CoordinatorWriteError) Error() string {
	return fmt.Sprintf("DKV: Could not write to the coordinator node. Write failed [%s]", string(e))
}

type MajorityWriteError string

func (e MajorityWriteError) Error() string {
	return fmt.Sprintf("DKV: Could not write to a majority of network nodes. Write failed [%s]", string(e))
}

type NonexistentKeyError string

func (e NonexistentKeyError) Error() string {
	return fmt.Sprintf("DKV: The desired key does not exist [%s]", string(e))
}

type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("DKV: Cannot connect to [%s]", string(e))
}

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

	var reply string

	outLog.Printf("Sending read to coordinator")
	err := c.Coordinator.Call("KVNode.CoordinatorRead", &key, &reply)
	if err != nil {
		outLog.Println("Could not connect to coordinator: ", err)
		return "", err
	}
	outLog.Printf("Successfully completed read")
	return reply, nil
}

// Write value to key
func (c CNode) Write(key, value string) error {
	args := struct {
		Key   string
		Value string
	}{
		key,
		value,
	}
	var reply int
	outLog.Printf("Sending write to coordinator")
	err := c.Coordinator.Call("KVNode.CoordinatorWrite", &args, &reply)
	if err != nil {
		outLog.Println("Could not connect to coordinator: ", err)
		return err
	}
	outLog.Printf("Successfully completed write")
	return nil
}

// Update value of key
func (c CNode) Update(key, value string) error {
	//TODO
	return nil
}

// Delete key-value pair
func (c CNode) Delete(key string) error {
	var reply int

	outLog.Printf("Sending delete to coordinator")
	err := c.Coordinator.Call("KVNode.CoordinatorDelete", &key, &reply)
	if err != nil {
		outLog.Println("Could not connect to coordinator: ", err)
		return err
	}
	outLog.Printf("Successfully completed delete")
	return nil
}
