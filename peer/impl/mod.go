package impl

import (
	"errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Node counter
var nodeCounter int64 = 0

// Logging setup
var logout = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
}

const lvl = "error"
const defaultLevel = zerolog.NoLevel
var Logger = zerolog.New(logout).Level(zerolog.NoLevel).With().Timestamp().Logger().With().Caller().Logger()

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

		log.Info().Msgf(chatMsg.String())
		return nil
	})

	atomic.AddInt64(&nodeCounter, 1)

	return &node{
		conf:         conf,
		routingTable: cRoutingTable{
			RWMutex:  sync.RWMutex{},
			m:        map[string]string{conf.Socket.GetAddress(): conf.Socket.GetAddress()},
		},
		name:         "node" + strconv.FormatInt(atomic.LoadInt64(&nodeCounter), 10),
		active: 	  false,
		quit: make(chan bool),
	}
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	quit         chan bool
	active       bool
	conf         peer.Configuration
	routingTable cRoutingTable
	name         string
}

type cRoutingTable struct {
	sync.RWMutex
	m peer.RoutingTable
}

// Start implements peer.Service
// TODO: do we need to crash if a packet if not processed correctly?
func (n *node) Start() error {
	if n.active {
		return xerrors.Errorf("%s is already running", n.name)
	}

	n.active = true

	go func() {
		Loop:
			for {
				// Shut down node if signalled
				select {
				case <- n.quit:
					break Loop
				default:
					// Block until packet is received
					pkt, err := n.conf.Socket.Recv(time.Second * 1)
					if errors.Is(err, transport.TimeoutErr(0)) {
						log.Warn().Msgf(err.Error())
						continue
					}

					// If addressed to us, process packet, crash if something goes wrong
					if pkt.Header.Destination == n.conf.Socket.GetAddress() {
						err := n.conf.MessageRegistry.ProcessPacket(pkt)
						if err != nil {
							log.Err(err)
							break
						}

						// Otherwise, relay packet, crash if something goes wrong
					} else {
						pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
						err := n.conf.Socket.Send(pkt.Header.Destination, pkt, 0)
						if err != nil {
							log.Err(err)
							break
						}
					}
				}
			}

		n.active = false

		if socket, ok := n.conf.Socket.(transport.ClosableSocket); ok {
			err := socket.Close()
			if err != nil {
				log.Err(err)
			}
		}

		return
	}()

	// Starting cannot really produce an error otherwise
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	n.quit <- true
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	n.routingTable.Lock()
	defer n.routingTable.Unlock()

	if _, ok := n.routingTable.m[dest]; !ok {
		return xerrors.Errorf("%s is not in the routing table of %s", dest, n.name)
	}

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)

	packet := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	err := n.conf.Socket.Send(n.routingTable.m[dest], packet, 0)
	return err
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	n.routingTable.Lock()
	defer n.routingTable.Unlock()

	for _, v := range addr {
		if v != n.conf.Socket.GetAddress() {
			n.routingTable.m[v] = v
		}
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.routingTable.Lock()
	defer n.routingTable.Unlock()

	clone := make(map[string]string)

	for k, v := range n.routingTable.m {
		clone[k] = v
	}

	return clone
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	n.routingTable.Lock()
	defer n.routingTable.Unlock()

	if relayAddr == "" {
		delete(n.routingTable.m, origin)
	} else {
		n.routingTable.m[origin] = relayAddr
	}
}

// Logging and error handling setup
func init() {
	var level zerolog.Level

	switch lvl {
	case "error":
		level = zerolog.ErrorLevel
	case "warn":
		level = zerolog.WarnLevel
	case "info":
		level = zerolog.InfoLevel
	case "debug":
		level = zerolog.DebugLevel
	case "trace":
		level = zerolog.TraceLevel
	case "":
		level = defaultLevel
	default:
		level = zerolog.TraceLevel
	}

	Logger = Logger.Level(level)
}
