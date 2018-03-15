package shared

import (
	"net"
)

// AddressesResponse
type AddressesResponse struct {
	Addresses []net.Addr
}
