package udp

import (
	"fmt"
	"golang.org/x/xerrors"
	"math"
	"net"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {

	// Format socket address as UDPAddr
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	// Start listening on given address
	connection, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	actualAddressRaw := connection.LocalAddr()
	actualAddress, err := net.ResolveUDPAddr("udp", actualAddressRaw.String())

	fmt.Printf("New socket now listening at %s\n", actualAddress)

	return &Socket{
		UDP:      n,
		address:  actualAddress,
		ins:      packets{},
		outs:     packets{},
		listener: connection,
		closed:   false,
	}, nil
	//addr, err := net.ResolveUDPAddr("udp", address)
	//if err != nil {
	//	return nil, err //TODO
	//}
	//
	//return &Socket{
	//	address:   addr,
	//	ins:       make([]transport.Packet, 0),
	//	outs:      make([]transport.Packet, 0),
	//	listening: false,
	//}, nil //TODO
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	*UDP
	address *net.UDPAddr
	ins packets
	outs packets
	closed bool
	listener *net.UDPConn
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	s.closed = true
	err := s.listener.Close()
	return err
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	
	// Set max timeout
	if timeout == 0 {
		timeout = math.MaxInt64
	}
	
	// Transform packet to raw bytes
	marshalledPacket, err := pkt.Marshal()
	if err != nil {
		return err
	}
	if len(marshalledPacket) > 1024 {
		return xerrors.Errorf("Message exceeds buffer size limit")
	}
	
	// Format destination address as UDPAddr
	d, err := net.ResolveUDPAddr("udp", dest)
	if err != nil {
		return xerrors.Errorf("%s is not a valid address", dest)
	}

	// Establish UDP socket in OS
	connection, err := net.DialUDP("udp", nil, d)
	defer connection.Close()
	if err != nil {
		return err
	}

	fmt.Printf("Sending packet to %s ...", connection.RemoteAddr())

	// Delegate sending packet to OS, return error if timeout is reached //TODO
	_, err = connection.Write(marshalledPacket)
	if err != nil {
		return err
	}

	fmt.Println(" done")
	s.outs.add(pkt)

	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {

	// Check if socket is closed
	if s.closed {
		return transport.Packet{}, xerrors.Errorf("Socket is closed")
	}

	// Create read buffer
	buf := make([]byte, 1024)

	// Block and wait to receive packet
	// Calling Close on the socket unblocks this and returns an error
	// If takes longer than timeout, ignore

	pktChan := make(chan transport.Packet, 1)

	go func() {
		_, _, err := s.listener.ReadFromUDP(buf)
		if err != nil {
			return
		}

		fmt.Printf("Reading incoming packet at %s", s.listener.LocalAddr())

		pkt := transport.Packet{}
		err = pkt.Unmarshal(buf)
		if err != nil {
			fmt.Println(err)
			return
		}


		pktChan <- pkt
	}()

	select {
	case pkt := <- pktChan:
		return pkt, nil
	case <- time.After(timeout):
		return transport.Packet{}, transport.TimeoutErr(timeout)
	}
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.address.String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs.getAll()
}

type Traffic struct {
	sync.Mutex

	items []item
}

type item struct {
	from          string
	to            string
	typeStr       string
	pkt           transport.Packet
	globalCounter int
	typeCounter   int
}

type packets struct {
	sync.Mutex
	data []transport.Packet
}

func (p *packets) add(pkt transport.Packet) {
	p.Lock()
	defer p.Unlock()

	p.data = append(p.data, pkt.Copy())
}

func (p *packets) getAll() []transport.Packet {
	p.Lock()
	defer p.Unlock()

	res := make([]transport.Packet, len(p.data))

	for i, pkt := range p.data {
		res[i] = pkt.Copy()
	}

	return res
}

