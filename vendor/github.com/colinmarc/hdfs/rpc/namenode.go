package rpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	hadoop "github.com/colinmarc/hdfs/protocol/hadoop_common"
	"github.com/golang/protobuf/proto"
)

const (
	rpcVersion            = 0x09
	serviceClass          = 0x0
	authProtocol          = 0x0
	protocolClass         = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
	protocolClassVersion  = 1
	handshakeCallID       = -3
	standbyExceptionClass = "org.apache.hadoop.ipc.StandbyException"
)

const backoffDuration = time.Second * 5

// NamenodeConnection represents an open connection to a namenode.
type NamenodeConnection struct {
	clientId         []byte
	clientName       string
	currentRequestID int
	user             string
	conn             net.Conn
	host             *namenodeHost
	hostList         []*namenodeHost
	reqLock          sync.Mutex
}

// NamenodeConnectionOptions represents the configurable options available
// for a NamenodeConnection.
type NamenodeConnectionOptions struct {
	Addresses []string
	User      string
}

// NamenodeError represents an interepreted error from the Namenode, including
// the error code and the java backtrace.
type NamenodeError struct {
	Method    string
	Message   string
	Code      int
	Exception string
}

// Desc returns the long form of the error code, as defined in the
// RpcErrorCodeProto in RpcHeader.proto
func (err *NamenodeError) Desc() string {
	return hadoop.RpcResponseHeaderProto_RpcErrorCodeProto_name[int32(err.Code)]
}

func (err *NamenodeError) Error() string {
	s := fmt.Sprintf("%s call failed with %s", err.Method, err.Desc())
	if err.Exception != "" {
		s += fmt.Sprintf(" (%s)", err.Exception)
	}

	return s
}

type namenodeHost struct {
	address     string
	lastError   error
	lastErrorAt time.Time
}

// NewNamenodeConnection creates a new connection to a namenode and performs an
// initial handshake.
//
// You probably want to use hdfs.New instead, which provides a higher-level
// interface.
func NewNamenodeConnection(address string, user string) (*NamenodeConnection, error) {
	return NewNamenodeConnectionWithOptions(NamenodeConnectionOptions{
		Addresses: []string{address},
		User:      user,
	})
}

// NewNamenodeConnectionWithOptions creates a new connection to a namenode with
// the given options and performs an initial handshake.
func NewNamenodeConnectionWithOptions(options NamenodeConnectionOptions) (*NamenodeConnection, error) {
	// Build the list of hosts to be used for failover.
	hostList := make([]*namenodeHost, len(options.Addresses))
	for i, addr := range options.Addresses {
		hostList[i] = &namenodeHost{address: addr}
	}

	// The ClientID is reused here both in the RPC headers (which requires a
	// "globally unique" ID) and as the "client name" in various requests.
	clientId := newClientID()
	c := &NamenodeConnection{
		clientId:   clientId,
		clientName: "go-hdfs-" + string(clientId),
		user:       options.User,
		hostList:   hostList,
	}

	err := c.resolveConnection()
	if err != nil {
		return nil, err
	}

	return c, nil
}

// WrapNamenodeConnection wraps an existing net.Conn to a Namenode, and preforms
// an initial handshake.
//
// Deprecated: use the higher-level hdfs.New or NewNamenodeConnection instead.
func WrapNamenodeConnection(conn net.Conn, user string) (*NamenodeConnection, error) {
	// The ClientID is reused here both in the RPC headers (which requires a
	// "globally unique" ID) and as the "client name" in various requests.
	clientId := newClientID()
	c := &NamenodeConnection{
		clientId:   clientId,
		clientName: "go-hdfs-" + string(clientId),
		user:       user,
		conn:       conn,
		host:       &namenodeHost{},
		hostList:   make([]*namenodeHost, 0),
	}

	err := c.writeNamenodeHandshake()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("Error performing handshake: %s", err)
	}

	return c, nil
}

func (c *NamenodeConnection) resolveConnection() error {
	if c.conn != nil {
		return nil
	}

	var err error

	if c.host != nil {
		err = c.host.lastError
	}

	for _, host := range c.hostList {
		if host.lastErrorAt.After(time.Now().Add(-backoffDuration)) {
			continue
		}

		c.host = host
		c.conn, err = net.DialTimeout("tcp", host.address, connectTimeout)
		if err != nil {
			c.markFailure(err)
			continue
		}

		err = c.writeNamenodeHandshake()
		if err != nil {
			c.markFailure(err)
			continue
		}

		break
	}

	if c.conn == nil {
		return fmt.Errorf("no available namenodes: %s", err)
	}

	return nil
}

func (c *NamenodeConnection) markFailure(err error) {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.host.lastError = err
	c.host.lastErrorAt = time.Now()
}

// ClientName provides a unique identifier for this client, which is required
// for various RPC calls. Confusingly, it's separate from clientID, which is
// used in the RPC header; to make things simpler, it reuses the random bytes
// from that, but adds a prefix to make it human-readable.
func (c *NamenodeConnection) ClientName() string {
	return c.clientName
}

// Execute performs an rpc call. It does this by sending req over the wire and
// unmarshaling the result into resp.
func (c *NamenodeConnection) Execute(method string, req proto.Message, resp proto.Message) error {
	c.reqLock.Lock()
	defer c.reqLock.Unlock()

	c.currentRequestID++

	for {
		err := c.resolveConnection()
		if err != nil {
			return err
		}

		err = c.writeRequest(method, req)
		if err != nil {
			c.markFailure(err)
			continue
		}

		err = c.readResponse(method, resp)
		if err != nil {
			// Only retry on a standby exception.
			if nerr, ok := err.(*NamenodeError); ok && nerr.Exception == standbyExceptionClass {
				c.markFailure(err)
				continue
			}

			return err
		}

		break
	}

	return nil
}

// RPC definitions

// A request packet:
// +-----------------------------------------------------------+
// |  uint32 length of the next three parts                    |
// +-----------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                    |
// +-----------------------------------------------------------+
// |  varint length + RequestHeaderProto                       |
// +-----------------------------------------------------------+
// |  varint length + Request                                  |
// +-----------------------------------------------------------+
func (c *NamenodeConnection) writeRequest(method string, req proto.Message) error {
	rrh := newRPCRequestHeader(c.currentRequestID, c.clientId)
	rh := newRequestHeader(method)

	reqBytes, err := makeRPCPacket(rrh, rh, req)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(reqBytes)
	return err
}

// A response from the namenode:
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcResponseHeaderProto                   |
// +-----------------------------------------------------------+
// |  varint length + Response                                 |
// +-----------------------------------------------------------+
func (c *NamenodeConnection) readResponse(method string, resp proto.Message) error {
	var packetLength uint32
	err := binary.Read(c.conn, binary.BigEndian, &packetLength)
	if err != nil {
		return err
	}

	packet := make([]byte, packetLength)
	_, err = io.ReadFull(c.conn, packet)
	if err != nil {
		return err
	}

	rrh := &hadoop.RpcResponseHeaderProto{}
	err = readRPCPacket(packet, rrh, resp)

	if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		return &NamenodeError{
			Method:    method,
			Message:   rrh.GetErrorMsg(),
			Code:      int(rrh.GetErrorDetail()),
			Exception: rrh.GetExceptionClassName(),
		}
	} else if int(rrh.GetCallId()) != c.currentRequestID {
		return errors.New("Error reading response: unexpected sequence number")
	}

	return nil
}

// A handshake packet:
// +-----------------------------------------------------------+
// |  Header, 4 bytes ("hrpc")                                 |
// +-----------------------------------------------------------+
// |  Version, 1 byte (default verion 0x09)                    |
// +-----------------------------------------------------------+
// |  RPC service class, 1 byte (0x00)                         |
// +-----------------------------------------------------------+
// |  Auth protocol, 1 byte (Auth method None = 0x00)          |
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                    |
// +-----------------------------------------------------------+
// |  varint length + IpcConnectionContextProto                |
// +-----------------------------------------------------------+
func (c *NamenodeConnection) writeNamenodeHandshake() error {
	rpcHeader := []byte{
		0x68, 0x72, 0x70, 0x63, // "hrpc"
		rpcVersion, serviceClass, authProtocol,
	}

	rrh := newRPCRequestHeader(handshakeCallID, c.clientId)
	cc := newConnectionContext(c.user)
	packet, err := makeRPCPacket(rrh, cc)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(append(rpcHeader, packet...))
	return err
}

// Close terminates all underlying socket connections to remote server.
func (c *NamenodeConnection) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func newRPCRequestHeader(id int, clientID []byte) *hadoop.RpcRequestHeaderProto {
	return &hadoop.RpcRequestHeaderProto{
		RpcKind:  hadoop.RpcKindProto_RPC_PROTOCOL_BUFFER.Enum(),
		RpcOp:    hadoop.RpcRequestHeaderProto_RPC_FINAL_PACKET.Enum(),
		CallId:   proto.Int32(int32(id)),
		ClientId: clientID,
	}
}

func newRequestHeader(methodName string) *hadoop.RequestHeaderProto {
	return &hadoop.RequestHeaderProto{
		MethodName:                 proto.String(methodName),
		DeclaringClassProtocolName: proto.String(protocolClass),
		ClientProtocolVersion:      proto.Uint64(uint64(protocolClassVersion)),
	}
}

func newConnectionContext(user string) *hadoop.IpcConnectionContextProto {
	return &hadoop.IpcConnectionContextProto{
		UserInfo: &hadoop.UserInformationProto{
			EffectiveUser: proto.String(user),
		},
		Protocol: proto.String(protocolClass),
	}
}
