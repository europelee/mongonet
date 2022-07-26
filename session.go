package mongonet

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Session struct {
	tcpServer *TCPServer
	conn      io.ReadWriteCloser
	tlsConn   *tls.Conn

	grpcServer       *GRPCServer
	stream           grpc.ServerStream
	incomingMetaData metadata.MD

	logger        *slogger.Logger
	remoteAddr    net.Addr
	SSLServerName string

	proxiedConnection   bool
	privateEndpointInfo PrivateEndpointInfo

	sticky bool
}

var ErrUnknownOpcode = errors.New("unknown opcode")

// ------------------

func NewTCPSession(s *TCPServer, proxyProtoConn *Conn) *Session {
	remoteAddr := proxyProtoConn.RemoteAddr()
	return &Session{
		tcpServer:           s,
		remoteAddr:          remoteAddr,
		logger:              s.loggerFactory(fmt.Sprintf("Session %s", remoteAddr)),
		proxiedConnection:   proxyProtoConn.IsProxied(),
		privateEndpointInfo: proxyProtoConn.PrivateEndpointInfo(),
	}
}

func NewGRPCSession(s *GRPCServer, stream grpc.ServerStream, serverName string, remoteAddr net.Addr, incomingMetaData metadata.MD) *Session {
	return &Session{
		grpcServer:       s,
		stream:           stream,
		SSLServerName:    serverName,
		remoteAddr:       remoteAddr,
		incomingMetaData: incomingMetaData,
		logger:           s.loggerFactory(fmt.Sprintf("Session %s", remoteAddr)),
	}
}

func (s *Session) IsTCPSession() bool {
	return s.tcpServer != nil
}

func (s *Session) IsGRPCSession() bool {
	return s.grpcServer != nil
}

func (s *Session) IsProxied() bool {
	return s.proxiedConnection
}

func (s *Session) Connection() io.ReadWriteCloser {
	return s.conn
}

func (s *Session) GetTLSConnection() *tls.Conn {
	return s.tlsConn
}

func (s *Session) GetPrivateEndpointInfo() PrivateEndpointInfo {
	return s.privateEndpointInfo
}

func (s *Session) Logf(level slogger.Level, messageFmt string, args ...interface{}) (*slogger.Log, []error) {
	return s.logger.Logf(level, messageFmt, args...)
}

func (s *Session) RemoteAddr() net.Addr {
	return s.remoteAddr
}

func (s *Session) SetRemoteAddr(v net.Addr) {
	s.remoteAddr = v
}

func (s *Session) SetPrivateEndpointId(id string) {
	s.privateEndpointInfo.privateEndpointId = id
}

func (s *Session) IsSticky() bool {
	return s.sticky
}

func (s *Session) SetSticky(sticky bool) {
	s.sticky = sticky
}

func (s *Session) IncomingMetaData() metadata.MD {
	return s.incomingMetaData
}

func (s *Session) SendMessage(m Message) error {
	if s.IsTCPSession() {
		return SendMessage(m, s.conn)
	} else {
		if s.IsSticky() {
			trailer := metadata.New(map[string]string{
				"Sticky": "true",
			})
			s.Logf(slogger.DEBUG, "Setting trailer %v", trailer)
			s.stream.SetTrailer(trailer)
		}
		mBytes := m.Serialize()
		return s.stream.SendMsg(&mBytes)
	}
}

func (s *Session) ReadMessage() (Message, error) {
	if s.IsTCPSession() {
		return ReadMessage(s.conn)
	} else {
		return ReadMessageFromStream(s.stream)
	}
}

func ReadMessageFromStream(stream grpc.ServerStream) (Message, error) {
	var in []byte
	err := stream.RecvMsg(&in)
	if err != nil {
		return nil, err
	}

	return ReadMessageFromBytes(in)
}

// Run executes a TCPSession with a net.Conn
func (s *Session) RunWithConn(conn net.Conn) {
	var err error
	s.conn = conn

	var worker ServerWorker
	defer func() {
		s.conn.Close()
		if worker != nil {
			worker.Close()
		}

		// server has sessions that will receive from the sessionCtx.Done() channel
		// decrement the session wait group
		if _, ok := s.tcpServer.contextualWorkerFactory(); ok {
			s.tcpServer.sessionManager.sessionWG.Done()
		}
	}()

	switch c := conn.(type) {
	case *tls.Conn:
		s.tlsConn = c
		s.SSLServerName = strings.TrimSuffix(c.ConnectionState().ServerName, ".")
	}

	s.logger.Logf(slogger.INFO, "new connection SSLServerName [%s]", s.SSLServerName)

	defer s.logger.Logf(slogger.INFO, "socket closed")

	if cwf, ok := s.tcpServer.contextualWorkerFactory(); ok {
		worker, err = cwf.CreateWorkerWithContext(s, s.tcpServer.sessionManager.ctx)
	} else {
		worker, err = s.tcpServer.workerFactory.CreateWorker(s)
	}

	if err != nil {
		s.logger.Logf(slogger.WARN, "error creating worker %v", err)
		return
	}

	worker.DoLoopTemp(nil)
}

// Run executes a GRPCSession with a grpc.ServerStream
func (s *Session) RunWithStream(worker ServerWorker, m Message) error {
	worker.DoLoopTemp(m)
	if !s.IsSticky() {
		s.logger.Logf(slogger.DEBUG, "Closing current worker session")
		worker.Close()
	}
	return nil
}

// UnpinByTransaction should be called by user code to unpin a pinned session for transactions
func (s *Session) UnpinByTransaction(lsid bson.D, txnNumber int64) {
	if !s.IsGRPCSession() {
		return
	}
	found := s.grpcServer.UnpinSessionByTransaction(s.SSLServerName, lsid, txnNumber)
	if found {
		s.Logf(slogger.DEBUG, "Unpinned session by transaction %v/%v", lsid, txnNumber)
		s.SetSticky(false)
	}
}

// UnpinByCursorId should be called by user code to unpin a pinned session for cursors
func (s *Session) UnpinByCursorId(cursorId int64) {
	if !s.IsGRPCSession() {
		return
	}
	found := s.grpcServer.UnpinSessionByCursorId(s.SSLServerName, cursorId)
	if found {
		s.Logf(slogger.DEBUG, "Unpinned session by cursorId %v", cursorId)
		s.SetSticky(false)
	}
}

func (s *Session) RespondToCommandMakeBSON(clientMessage Message, args ...interface{}) error {
	if len(args)%2 == 1 {
		return fmt.Errorf("magic bson has to be even # of args, got %d", len(args))
	}

	gotOk := false

	doc := bson.D{}
	for idx := 0; idx < len(args); idx += 2 {
		name, ok := args[idx].(string)
		if !ok {
			return fmt.Errorf("got a non string for bson name: %t", args[idx])
		}
		doc = append(doc, bson.E{name, args[idx+1]})
		if name == "ok" {
			gotOk = true
		}
	}

	if !gotOk {
		doc = append(doc, bson.E{"ok", 1})
	}

	doc2, err := SimpleBSONConvert(doc)
	if err != nil {
		return err
	}
	return s.RespondToCommand(clientMessage, doc2)
}

// do not call with OP_GET_MORE since we never added support for that
func (s *Session) RespondToCommand(clientMessage Message, doc SimpleBSON) error {
	switch clientMessage.Header().OpCode {

	case OP_QUERY:
		rm := &ReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_REPLY},
			0, // flags - error bit
			0, // cursor id
			0, // StartingFrom
			1, // NumberReturned
			[]SimpleBSON{doc},
		}
		return s.SendMessage(rm)

	case OP_INSERT, OP_UPDATE, OP_DELETE:
		// For MongoDB 2.6+, and wpv 3+, these are only used for unacknowledged writes, so do nothing
		return nil

	case OP_COMMAND:
		rm := &CommandReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_COMMAND_REPLY},
			doc,
			SimpleBSONEmpty(),
			[]SimpleBSON{},
		}
		return s.SendMessage(rm)

	case OP_MSG:
		rm := &MessageMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_MSG},
			0,
			[]MessageMessageSection{
				&BodySection{
					doc,
				},
			},
		}
		return s.SendMessage(rm)

	case OP_GET_MORE:
		return errors.New("Internal error.  Should not be passing a GET_MORE message here.")

	default:
		return ErrUnknownOpcode
	}

}

func (s *Session) RespondWithError(clientMessage Message, err error) error {
	s.Logf(slogger.INFO, "RespondWithError %v", err)
	var errBSON bson.D
	if err == nil {
		errBSON = bson.D{{"ok", 1}}
	} else if mongoErr, ok := err.(MongoError); ok {
		errBSON = mongoErr.ToBSON()
	} else {
		errBSON = bson.D{{"ok", 0}, {"errmsg", err.Error()}}
	}

	doc, myErr := SimpleBSONConvert(errBSON)
	if myErr != nil {
		return myErr
	}

	switch clientMessage.Header().OpCode {
	case OP_QUERY, OP_GET_MORE:
		rm := &ReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_REPLY},

			// We should not set the error bit because we are
			// responding with errmsg instead of $err
			0, // flags - error bit

			0, // cursor id
			0, // StartingFrom
			1, // NumberReturned
			[]SimpleBSON{doc},
		}
		return s.SendMessage(rm)

	case OP_INSERT, OP_UPDATE, OP_DELETE:
		// For MongoDB 2.6+, and wpv 3+, these are only used for unacknowledged writes, so do nothing
		return nil

	case OP_COMMAND:
		rm := &CommandReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_COMMAND_REPLY},
			doc,
			SimpleBSONEmpty(),
			[]SimpleBSON{},
		}
		return s.SendMessage(rm)

	case OP_MSG:
		rm := &MessageMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_MSG},
			0,
			[]MessageMessageSection{
				&BodySection{
					doc,
				},
			},
		}
		return s.SendMessage(rm)

	default:
		return ErrUnknownOpcode
	}
}
