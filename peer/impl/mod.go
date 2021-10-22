package impl

import (
	"errors"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Node counter
var nodeCounter int64 = 0

// Logging setup
const defaultLevel = zerolog.InfoLevel

// Logging and error handling setup
func init() {
	lvl := "info"

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

var logout = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
}

var Logger = zerolog.New(logout).Level(zerolog.NoLevel).With().Timestamp().Logger().With().Caller().Logger()
var log zerolog.Logger

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	node := node{
		conf: conf,
		routingTable: ConcurrentRoutingTable{
			RWMutex: sync.RWMutex{},
			m:       map[string]string{conf.Socket.GetAddress(): conf.Socket.GetAddress()},
		},
		name:   "node" + strconv.FormatInt(atomic.LoadInt64(&nodeCounter), 10),
		active: false,
		quit:   make(chan bool),

		rumorCount: 0,
		status:     map[string]uint{},
	}

	atomic.AddInt64(&nodeCounter, 1)

	node.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, node.ChatMessageCallback)
	node.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, node.RumorsMessageCallback)
	node.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, node.StatusMessageCallback)
	node.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, node.AckMessageCallback)
	node.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, node.EmptyMessageCallback)
	node.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, node.PrivateMessageCallback)

	return &node
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
	routingTable ConcurrentRoutingTable
	name         string

	rumorCount uint
	status     map[string]uint
}

type ConcurrentRoutingTable struct {
	sync.RWMutex
	m peer.RoutingTable
}

func (n *node) ChatMessageCallback(message types.Message, packet transport.Packet) error {
	chatMsg, ok := message.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", message)
	}

	log.Info().Msgf("%s", chatMsg.Message)
	return nil
}

func (n *node) RumorsMessageCallback(message types.Message, packet transport.Packet) error {
	if packet.Header == nil {
		return xerrors.Errorf("%s: nested rumors are not suppoerted", n.GetAddress())
	}

	packetOrigin := packet.Header.Source

	rumorsMsg, ok := message.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", message)
	}

	// TODO: what the fuck do we do here
	forward := false
	excludeList := make([]string, 0)
	for _, rumor := range rumorsMsg.Rumors {
		embeddedPkt := transport.Packet{
			Header: nil,
			Msg:    rumor.Msg,
		}

		// Attempt to update status, continue if sequence is not expected
		if !n.UpdateStatus(rumor.Origin, rumor.Sequence) {
			continue
		}

		n.SetRoutingEntry(rumor.Origin, packet.Header.RelayedBy)
		excludeList = append(excludeList, rumor.Origin)

		forward = true

		err := n.conf.MessageRegistry.ProcessPacket(embeddedPkt)
		if err != nil {
			return xerrors.Errorf("%s: error processing rumor: %s", n.GetAddress(), err)
		}
	}

	if forward {
		excludeList = append(excludeList, packetOrigin)
		excludeList = append(excludeList, packet.Header.RelayedBy)
		dest := n.GetRandomNeighbour(excludeList...)

		if dest != "" {
			err := n.Unicast(dest, *packet.Msg)
			if err != nil {
				return xerrors.Errorf("%s: error forwarding rumor: %s", n.GetAddress(), err)
			}
		}
	}

	// Send ACK message to sender if it's not us
	if packetOrigin != n.GetAddress() {

		// Update our routing table
		// TODO apparently we should not do this, only the rumor's origin needs to be added
		//n.SetRoutingEntry(packetOrigin, packet.Header.RelayedBy)

		// Create and send ACK
		ackMsg := types.AckMessage{
			AckedPacketID: packet.Header.PacketID,
			Status:        n.status,
		}

		dest := packetOrigin

		ackMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(ackMsg)

		if err != nil {
			return xerrors.Errorf("%s: error marshalling ACK message: %s", n.GetAddress(), err)
		}

		//err = n.Unicast(dest, ackMsgMarshalled)
		header := transport.NewHeader(n.GetAddress(), n.GetAddress(), dest, 0)
		responsePacket := transport.Packet{
			Header: &header,
			Msg:    &ackMsgMarshalled,
		}

		n.conf.Socket.Send(dest, responsePacket, 0)

		if err != nil {
			return xerrors.Errorf("%s: error sending ACK message: %s", n.GetAddress(), err)
		}
	}

	return nil
}

func (n *node) StatusMessageCallback(message types.Message, packet transport.Packet) error {
	return nil
}

func (n *node) AckMessageCallback(message types.Message, packet transport.Packet) error {
	return nil
}

func (n *node) EmptyMessageCallback(message types.Message, packet transport.Packet) error {
	return nil
}

func (n *node) PrivateMessageCallback(message types.Message, packet transport.Packet) error {
	return nil
}

func (n *node) UpdateStatus(source string, newSequence uint) bool {

	// We update the status if either:
	//   1. The incoming rumor's sequence number is one larger than the previous
	//   2. There is no entry for rumors from this source yet and the sequence number is 1
	if oldSequence, ok := n.status[source]; (ok && oldSequence == newSequence-1) || (!ok && newSequence == 1) {
		n.status[source] = newSequence
		return true
	}

	return false
}

func (n *node) GetRandomNeighbour(exclude ...string) string {
	clonedRoutingTable := n.routingTable.CopyTable()

	// Delete ourselves
	delete(clonedRoutingTable, n.GetAddress())

	for _, v := range exclude {
		delete(clonedRoutingTable, v)
	}

	if len(clonedRoutingTable) == 0 {
		return ""
	}

	// Pick neighbour to send to
	randomIndex := rand.Intn(len(clonedRoutingTable))

	// Get keyset
	keys := make([]string, len(clonedRoutingTable))
	i := 0
	for k := range clonedRoutingTable {
		keys[i] = k
		i++
	}

	// Randomly index into keys
	return keys[randomIndex]
}

// Start implements peer.Service
// TODO: do we need to crash if a packet if not processed correctly?
func (n *node) Start() error {
	if n.active {
		return xerrors.Errorf("%s is already running", n.name)
	}

	n.active = true

	// Packet receiver
	go func() {
	Loop:
		for {
			// Shut down node if signalled
			select {
			case <-n.quit:
				break Loop
			default:
				// Block until packet is received
				pkt, err := n.conf.Socket.Recv(time.Second * 1)
				if errors.Is(err, transport.TimeoutErr(0)) {
					log.Warn().Msgf(err.Error())
					continue
				}

				// If addressed to us, process packet, crash if something goes wrong
				if pkt.Header.Destination == n.GetAddress() {
					err := n.conf.MessageRegistry.ProcessPacket(pkt)
					if err != nil {
						log.Err(err)
						break
					}

					// Otherwise, relay packet, crash if something goes wrong
				} else {
					pkt.Header.RelayedBy = n.GetAddress()
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

	// Anti-entropy
	go func() {
		if n.conf.AntiEntropyInterval == 0 {
			return
		}

		for {
			select {
			case <-n.quit:
				return
			default:
				time.Sleep(n.conf.AntiEntropyInterval)

				var statusMsg types.StatusMessage = Copy(n.status)
				statusMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(statusMsg)

				if err != nil {
					log.Warn().Msgf("%s: error marshalling anti-entropy: %s", n.GetAddress(), err)
					return
				}

				dest := n.GetRandomNeighbour()
				err = n.Unicast(dest, statusMsgMarshalled)
				if err != nil {
					log.Warn().Msgf("%s: error executing anti-entropy: %s", n.GetAddress(), err)
					return
				}
			}
		}
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

	header := transport.NewHeader(n.GetAddress(), n.GetAddress(), dest, 0)
	packet := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	err := n.conf.Socket.Send(n.routingTable.m[dest], packet, 0)
	return err
}

func (n *node) Broadcast(msg transport.Message) error {
	// create Rumor
	n.rumorCount++
	rumor := types.Rumor{
		Origin:   n.GetAddress(),
		Sequence: n.rumorCount,
		Msg:      &msg,
	}

	rumorsMsg := types.RumorsMessage{Rumors: []types.Rumor{rumor}}

	rumorsMessageMarshalled, err := n.conf.MessageRegistry.MarshalMessage(rumorsMsg)

	if err != nil {
		return xerrors.Errorf("error marshalling rumorsMessage: %s", err)
	}

	dest := n.GetRandomNeighbour()

	//// Create wrapper packet and send it
	header := transport.NewHeader(n.GetAddress(), n.GetAddress(), dest, 0)
	pkt := transport.Packet{
		Header: &header,
		Msg:    &rumorsMessageMarshalled,
	}

	//err = n.conf.Socket.Send(n.routingTable.m[dest], pkt, 0)

	//n.routingTable.Unlock()
	//n.Unicast(dest, rumorsMessageMarshalled)
	//n.Unicast(n.GetAddress(), rumorsMessageMarshalled)

	if err != nil {
		return xerrors.Errorf("error broadcasting message: %s", err)
	}

	// Process the rumor for ourselves
	err = n.conf.MessageRegistry.ProcessPacket(pkt)

	return err
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, v := range addr {
		if v != n.GetAddress() {
			n.routingTable.Add(v, v)
		}
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	return n.routingTable.CopyTable()
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		n.routingTable.Remove(origin)
	} else {
		n.routingTable.Add(origin, relayAddr)
	}
}

func (n *node) GetAddress() string {
	return n.conf.Socket.GetAddress()
}

func (table *ConcurrentRoutingTable) Add(src, dst string) {
	table.Lock()
	defer table.Unlock()
	table.m[src] = dst
}

func (table *ConcurrentRoutingTable) Get(dst string) string {
	table.Lock()
	defer table.Unlock()
	if val, ok := table.m[dst]; ok {
		return val
	}
	return ""
}

func (table *ConcurrentRoutingTable) Remove(src string) {
	table.Lock()
	defer table.Unlock()
	delete(table.m, src)
}

func (table *ConcurrentRoutingTable) CopyTable() peer.RoutingTable {
	copiedTable := make(map[string]string)
	table.Lock()
	defer table.Unlock()

	for index, element := range table.m {
		copiedTable[index] = element
	}

	return copiedTable
}

func Copy(m map[string]uint) map[string]uint {
	clone := make(map[string]uint, len(m))

	for k, v := range m {
		clone[k] = v
	}

	return clone
}
