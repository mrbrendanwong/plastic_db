package dkvapi

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

// Represent a Coordinator node
type CNode interface {
	Read(key string) (string, error)
	Write(key, value string) error
	Update(key, value string) error
	Delete(key string) error
}

///////////////////////////////////////////////////////////////////////////
// CLIENT-COORDINATOR FUNCTIONS
///////////////////////////////////////////////////////////////////////////

// Get value of key
func (c *CNode) Read(key string) (string, error) {
	// TODO
}

// Write value to key
func (c *CNode) Write(key, value string) error {
	//TODO
}

// Update value of key
func (c *CNode) Update(key, value string) error {
	//TODO
}

// Delete key-value pair
func (c *CNode) Delete(key, value string) error {
	//TODO
}
