package impl

import (
	"errors"
	"fmt"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, func(message types.Message, packet transport.Packet) error {
		chatMsg, ok := message.(*types.ChatMessage)
		if !ok {
			return xerrors.Errorf("wrong type: %T", message)
		}

		fmt.Println(chatMsg)
		return nil
	})

	return &node{
		conf:         conf,
		routingTable: map[string]string{conf.Socket.GetAddress(): conf.Socket.GetAddress()},
		neighbours:   make([]string, 0),
	}
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	quit         bool
	conf         peer.Configuration
	routingTable peer.RoutingTable
	neighbours   []string
}

// Start implements peer.Service
func (n *node) Start() error {
	go func() {
		for {
			if n.quit {
				return
			}

			pkt, err := n.conf.Socket.Recv(0)
			if errors.Is(err, transport.TimeoutErr(0)) {
				continue
			}

			if pkt.Header.Destination == n.conf.Socket.GetAddress() {
				err := n.conf.MessageRegistry.ProcessPacket(pkt)
				if err != nil {
					return //TODO
				}
			} else {
				pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
				err := n.conf.Socket.Send(pkt.Header.Destination, pkt, 0)
				if err != nil {
					return //TODO
				}
			}

		}
	}()

	return nil //TODO
}

// Stop implements peer.Service
func (n *node) Stop() error {
	n.quit = true
	return nil //TODO
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {

	for k, v := range n.routingTable {
		fmt.Println(k, v)
	}

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)

	packet := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	err := n.conf.Socket.Send(dest, packet, 0)
	return err
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, v := range addr {
		if v != n.conf.Socket.GetAddress() {
			n.neighbours = append(n.neighbours, v)
			n.routingTable[v] = v
		}
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	clone := make(map[string]string)

	for k, v := range n.routingTable {
		clone[k] = v
	}

	return clone
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		delete(n.routingTable, origin)
	} else {
		n.routingTable[origin] = relayAddr
	}

	if origin == relayAddr {
		n.neighbours = append(n.neighbours, origin)
	}
}
