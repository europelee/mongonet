package mongonet

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

const TransactionNotFoundErrorCode = 251
const CursorNotFoundErrorCode = 43

// ServerWorker interface abstracts proxy session processing
type ServerWorker interface {
	DoLoopTemp(m Message)
	Close()
}

type ServerWorkerFactory interface {
	CreateWorker(session *Session) (ServerWorker, error)
}

type LoggerFactory func(prefix string) *slogger.Logger

// ServerWorkerWithContextFactory should be used when workers need to listen to the Done channel of the session context.
// The server will cancel the ctx passed to CreateWorkerWithContext() when it exits; causing the ctx's Done channel to be closed
// Implementing this interface will cause the server to incrememnt a session wait group when each new session starts.
// A mongonet session will decrement the wait group after calling .Close() on the session.
// When using this you should make sure that your `DoLoopTemp` returns when it receives from the context Done Channel.
type ServerWorkerWithContextFactory interface {
	ServerWorkerFactory
	CreateWorkerWithContext(session *Session, ctx context.Context) (ServerWorker, error)
}

type SyncTlsConfig struct {
	lock      sync.RWMutex
	tlsConfig *tls.Config
}

func NewSyncTlsConfig() *SyncTlsConfig {
	return &SyncTlsConfig{
		sync.RWMutex{},
		&tls.Config{},
	}
}

func (s *SyncTlsConfig) getTlsConfig() *tls.Config {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.tlsConfig
}

func (s *SyncTlsConfig) SetTlsConfig(sslKeys []*SSLPair, cipherSuites []uint16, minTlsVersion uint16, fallbackKeys []SSLPair) (ok bool, names []string, errs []error) {
	ok = true
	certs := []tls.Certificate{}
	for _, pair := range fallbackKeys {
		cer, err := tls.LoadX509KeyPair(pair.Cert, pair.Key)
		if err != nil {
			return false, []string{}, append(errs, fmt.Errorf("cannot load fallback certificate from files %s, %s. Error: %v", pair.Cert, pair.Key, err))
		}
		certs = append(certs, cer)
	}

	for _, pair := range sslKeys {
		cer, err := tls.X509KeyPair([]byte(pair.Cert), []byte(pair.Key))
		if err != nil {
			ok = false
			errs = append(errs, fmt.Errorf("cannot parse certificate %v. err=%v", pair.Id, err))
			continue
		}
		certs = append(certs, cer)
	}

	tlsConfig := &tls.Config{Certificates: certs}

	if minTlsVersion != 0 {
		tlsConfig.MinVersion = minTlsVersion
	}

	if cipherSuites != nil {
		tlsConfig.CipherSuites = cipherSuites
	}

	tlsConfig.ClientAuth = tls.RequestClientCert

	s.lock.Lock()
	defer s.lock.Unlock()
	s.tlsConfig = tlsConfig
	s.tlsConfig.BuildNameToCertificate()
	for key := range s.tlsConfig.NameToCertificate {
		names = append(names, key)
	}
	return ok, names, errs
}

type TCPServerConfig struct {
	Enabled bool

	BindHost string
	BindPort int

	UseSSL        bool
	SSLKeys       []SSLPair
	SyncTlsConfig *SyncTlsConfig

	MinTlsVersion      uint16        // see tls.Version* constants
	TCPKeepAlivePeriod time.Duration // set to 0 for no keep alives

	CipherSuites []uint16
}

type GRPCServerConfig struct {
	Enabled bool

	BindHost string
	BindPort int
}

type sessionManager struct {
	sessionWG *sync.WaitGroup
	ctx       context.Context
}

type TCPServer struct {
	config         TCPServerConfig
	logger         *slogger.Logger
	loggerFactory  LoggerFactory
	workerFactory  ServerWorkerFactory
	ctx            context.Context
	cancelCtx      context.CancelFunc
	initChan       chan error
	doneChan       chan struct{}
	sessionManager *sessionManager
	net.Addr
}

// called by a synched method
func (s *TCPServer) OnSSLConfig(sslPairs []*SSLPair) (ok bool, names []string, errs []error) {
	return s.config.SyncTlsConfig.SetTlsConfig(sslPairs, s.config.CipherSuites, s.config.MinTlsVersion, s.config.SSLKeys)
}

func (s *TCPServer) Run() error {
	bindTo := fmt.Sprintf("%s:%d", s.config.BindHost, s.config.BindPort)

	s.logger.Logf(slogger.WARN, "listening on %s", bindTo)

	defer s.cancelCtx()
	defer close(s.initChan)

	keepAlive := time.Duration(-1)       // negative Duration means keep-alives disabled in ListenConfig
	if s.config.TCPKeepAlivePeriod > 0 { // but in our config we use 0 to mean keep-alives are disabled
		keepAlive = s.config.TCPKeepAlivePeriod
	}
	lc := &net.ListenConfig{KeepAlive: keepAlive}
	ln, err := lc.Listen(s.ctx, "tcp", bindTo)
	if err != nil {
		returnErr := NewStackErrorf("cannot start listening in proxy: %s", err)
		s.initChan <- returnErr
		return returnErr
	}
	s.Addr = ln.Addr()
	s.initChan <- nil

	defer func() {
		ln.Close()

		// add another context cancellation so that it happens now.
		// Otherwise the prior deferred cancellation won't happen until
		// after this defer call (because defers are called LIFO)
		s.cancelCtx()

		// wait for all sessions to end
		if s.sessionManager == nil {
			s.logger.Logf(slogger.WARN, "Not waiting for sessions to close because there is no session manager")
		} else {
			s.logger.Logf(slogger.WARN, "waiting for sessions to close...")
			s.sessionManager.sessionWG.Wait()
			s.logger.Logf(slogger.WARN, "done")
		}

		close(s.doneChan)
	}()

	type accepted struct {
		conn net.Conn
		err  error
	}

	incomingConnections := make(chan accepted, 128)

	go func() {
		for {
			conn, err := ln.Accept()
			incomingConnections <- accepted{conn, err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			return nil

		case connectionEvent := <-incomingConnections:
			if connectionEvent.err != nil {
				if s.ctx.Err() != nil {
					// context was cancelled.  Exit cleanly
					return nil
				}
				return NewStackErrorf("could not accept in proxy: %s", err)
			}
			go s.handleConnection(connectionEvent.conn)
		}
	}

}

func (s *TCPServer) handshake(conn net.Conn) (net.Conn, error) {
	if !s.config.UseSSL {
		return conn, nil
	}

	tlsConn := tls.Server(conn, s.config.SyncTlsConfig.getTlsConfig())
	if err := tlsConn.Handshake(); err != nil {
		return conn, err
	}
	return tlsConn, nil
}

func (s *TCPServer) handleConnection(origConn net.Conn) {
	proxyProtoConn, err := NewConn(origConn)
	if err != nil {
		s.logger.Logf(slogger.ERROR, "Error setting up a proxy protocol compatible connection: %v", err)
		origConn.Close()
		return
	}

	if proxyProtoConn.IsProxied() {
		s.logger.Logf(
			slogger.DEBUG,
			"accepted a proxied connection (local=%v, remote=%v, proxy=%v, target=%v, version=%v)",
			proxyProtoConn.LocalAddr(),
			proxyProtoConn.RemoteAddr(),
			proxyProtoConn.ProxyAddr(),
			proxyProtoConn.TargetAddr(),
			proxyProtoConn.Version(),
		)
	} else {
		s.logger.Logf(
			slogger.DEBUG,
			"accepted a regular connection (local=%v, remote=%v)",
			origConn.LocalAddr(),
			origConn.RemoteAddr(),
		)
	}
	newconn, err := s.handshake(proxyProtoConn)
	if err != nil {
		s.logger.Logf(slogger.ERROR, "TLS Handshake failed. err=%v", err)
		origConn.Close()
		return
	}
	c := NewTCPSession(s, proxyProtoConn)

	if _, ok := s.contextualWorkerFactory(); ok {
		s.sessionManager.sessionWG.Add(1)
	}

	c.RunWithConn(newconn)
}

// InitChannel returns a channel that will send nil once the server has started
// listening, or an error indicating why the server failed to start
func (s *TCPServer) InitChannel() <-chan error {
	return s.initChan
}

func (s *TCPServer) Close() {
	s.cancelCtx()
	<-s.doneChan
}

func NewTCPServer(config TCPServerConfig, serverWorkerFactory ServerWorkerFactory, loggerFactory LoggerFactory) TCPServer {
	ctx, cancelCtx := context.WithCancel(context.Background())
	return TCPServer{
		config,
		loggerFactory("TCPServer"),
		loggerFactory,
		serverWorkerFactory,
		ctx,
		cancelCtx,
		make(chan error, 1),
		make(chan struct{}),
		&sessionManager{
			&sync.WaitGroup{},
			ctx,
		},
		nil,
	}
}

func (s *TCPServer) contextualWorkerFactory() (ServerWorkerWithContextFactory, bool) {
	swf, ok := s.workerFactory.(ServerWorkerWithContextFactory)
	return swf, ok
}

// pinnedSessionKey is used to lookup pinned sessions
// transaction fields should be checked first as cursors as part of a transaction will already have been pinned
// Note: lsid (bson.D) is not a comparable field
type pinnedSessionKey struct {
	SSLServerName string
	lsid          bson.D
	txnNumber     int64
	cursorId      int64
}

type GRPCServer struct {
	config        GRPCServerConfig
	logger        *slogger.Logger
	loggerFactory LoggerFactory
	workerFactory ServerWorkerFactory
	grpcServer    *grpc.Server

	pinnedSessionMap     map[*pinnedSessionKey]ServerWorker
	pinnedSessionMapLock sync.RWMutex
}

func NewGRPCServer(config GRPCServerConfig, serverWorkerFactory ServerWorkerFactory, loggerFactory LoggerFactory) GRPCServer {
	return GRPCServer{
		config:        config,
		logger:        loggerFactory("GRPCServer"),
		loggerFactory: loggerFactory,
		workerFactory: serverWorkerFactory,
		grpcServer: grpc.NewServer(
			grpc.ForceServerCodec(BytesCodec{}),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Timeout: time.Minute * 5,
			})),
		pinnedSessionMap:     make(map[*pinnedSessionKey]ServerWorker),
		pinnedSessionMapLock: sync.RWMutex{},
	}
}

func (s *GRPCServer) PinSessionByTransaction(worker ServerWorker, SSLServerName string, txn *PinnedTransactionSession) {
	s.pinnedSessionMapLock.Lock()
	defer s.pinnedSessionMapLock.Unlock()

	s.pinnedSessionMap[&pinnedSessionKey{
		SSLServerName: SSLServerName,
		lsid:          txn.lsid,
		txnNumber:     txn.txnNumber,
	}] = worker
}

func (s *GRPCServer) PinSessionByCursorId(worker ServerWorker, SSLServerName string, cursorId int64) {
	s.pinnedSessionMapLock.Lock()
	defer s.pinnedSessionMapLock.Unlock()

	s.pinnedSessionMap[&pinnedSessionKey{
		SSLServerName: SSLServerName,
		cursorId:      cursorId,
	}] = worker
}

func (s *GRPCServer) LookupSessionByTransaction(SSLServerName string, lsid bson.D, txnNumber int64) (ServerWorker, bool) {
	s.pinnedSessionMapLock.RLock()
	defer s.pinnedSessionMapLock.RUnlock()

	for k, v := range s.pinnedSessionMap {
		if k.SSLServerName == SSLServerName &&
			k.txnNumber == txnNumber &&
			reflect.DeepEqual(k.lsid, lsid) {
			return v, true
		}
	}

	return nil, false
}

func (s *GRPCServer) LookupSessionByCursorId(SSLServerName string, cursorId int64) (ServerWorker, bool) {
	s.pinnedSessionMapLock.RLock()
	defer s.pinnedSessionMapLock.RUnlock()

	for k, v := range s.pinnedSessionMap {
		if k.SSLServerName == SSLServerName &&
			k.cursorId == cursorId {
			return v, true
		}
	}

	return nil, false
}

func (s *GRPCServer) UnpinSessionByTransaction(SSLServerName string, lsid bson.D, txnNumber int64) bool {
	s.pinnedSessionMapLock.Lock()
	defer s.pinnedSessionMapLock.Unlock()

	for k := range s.pinnedSessionMap {
		if k.SSLServerName == SSLServerName &&
			k.txnNumber == txnNumber &&
			reflect.DeepEqual(k.lsid, lsid) {
			delete(s.pinnedSessionMap, k)
			return true
		}
	}

	return false
}

func (s *GRPCServer) UnpinSessionByCursorId(SSLServerName string, cursorId int64) bool {
	s.pinnedSessionMapLock.Lock()
	defer s.pinnedSessionMapLock.Unlock()

	for k := range s.pinnedSessionMap {
		// don't unpin until UnpinSessionByTransaction
		if k.txnNumber != 0 {
			continue
		}
		if k.SSLServerName == SSLServerName &&
			k.cursorId == cursorId {
			delete(s.pinnedSessionMap, k)
			return true
		}
	}

	return false
}

func (s *GRPCServer) Run() error {
	s.grpcServer.RegisterService(&grpc.ServiceDesc{
		ServiceName: "mongonet",
		HandlerType: nil,
		Methods:     []grpc.MethodDesc{},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "Send",
				ServerStreams: true,
				Handler: func(srv interface{}, stream grpc.ServerStream) error {
					peer, ok := peer.FromContext(stream.Context())
					if !ok {
						return fmt.Errorf("Unable to retrieve peer from context")
					}
					md, ok := metadata.FromIncomingContext(stream.Context())
					if !ok {
						return fmt.Errorf("Unable to retrieve metadata from context")
					}
					s.logger.Logf(
						slogger.DEBUG,
						"accepted a GRPC server stream connection (peer=%v, metadata=%v)",
						peer,
						md,
					)

					// Attempt to specify TLS ServerName field via default metadata field name
					var serverName string
					serverNameMD := md.Get("ServerName")
					if len(serverNameMD) >= 1 {
						serverName = serverNameMD[0]
					} else {
						return fmt.Errorf("unable to retrieve expected ServerName incoming metadata")
					}

					remoteAddr := peer.Addr
					// Attempt to specify RemoteAddr field via default metadata field name
					remoteAddrMD := md.Get("RemoteAddr")
					if len(remoteAddrMD) >= 1 {
						var err error
						resolvedAddr, err := net.ResolveTCPAddr("tcp", remoteAddrMD[0])
						if err != nil {
							s.logger.Logf(
								slogger.WARN,
								"Error resolving RemoteAddr metadata: %v %v",
								remoteAddrMD[0],
								err,
							)
						} else {
							remoteAddr = resolvedAddr
						}
					} else {
						s.logger.Logf(slogger.WARN, "Expected RemoteAddr incoming metadata was not present")
					}

					worker, m, err := s.lookupPinnedSession(serverName, stream)
					if err != nil {
						if mongoErr, ok := err.(MongoError); ok {
							s.logger.Logf(slogger.ERROR, "error looking up pinned session %v", mongoErr)
							session := NewGRPCSession(s, stream, serverName, remoteAddr, md)
							session.RespondWithError(m, mongoErr)
							return nil
						}
						return fmt.Errorf("error looking up pinned session %v", err)
					}
					if worker != nil {
						ps, ok := worker.(*ProxySession)
						if !ok {
							return fmt.Errorf("invalid session")
						}
						ps.stream = stream
						return ps.RunWithStream(worker, m)
					} else {
						session := NewGRPCSession(s, stream, serverName, remoteAddr, md)
						worker, err = s.workerFactory.CreateWorker(session)
						if err != nil {
							return fmt.Errorf("error creating worker %v", err)
						}
						return session.RunWithStream(worker, m)
					}
				},
			},
		},
	}, nil)

	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%d", s.config.BindHost, s.config.BindPort))
	if err != nil {
		return err
	}

	s.logger.Logf(slogger.WARN, "Starting local gRPC server %v\n", lis.Addr())
	if err := s.grpcServer.Serve(lis); err != nil {
		return err
	}

	return nil
}

func (s *GRPCServer) GracefulStop() {
	s.grpcServer.GracefulStop()
}

// lookupPinnedSession attempts to find any pinned sessions
// if successful, it also returns the initial read message for upstream processing
func (s *GRPCServer) lookupPinnedSession(SSLServerName string, stream grpc.ServerStream) (ServerWorker, Message, error) {
	m, err := ReadMessageFromStream(stream)
	if err != nil {
		s.logger.Logf(slogger.DEBUG, "Error reading initial message from stream %v", err)
		return nil, nil, err
	}

	switch message := m.(type) {
	case *MessageMessage:
		bodyDoc, err := message.BodyDoc()
		if err != nil {
			return nil, nil, err
		}
		simpleBSON, err := SimpleBSONConvert(bodyDoc)
		if err != nil {
			return nil, nil, err
		}
		cmd, err := simpleBSON.ToBSOND()
		if err != nil {
			return nil, nil, err
		}

		// Attempt to lookup a pinned session if not starting a new transaction
		// Multi-statement transactions begin with "startTransaction:true" and each operation includes "autocommit: false"
		// Single statement retryable writes may contain txnNumber/lsid but will not include "autocommit:false".
		startTransactionIdx := BSONIndexOf(cmd, "startTransaction")
		autocommitIdx := BSONIndexOf(cmd, "autocommit")
		if startTransactionIdx < 0 && autocommitIdx >= 0 {
			lsidIdx := BSONIndexOf(cmd, "lsid")
			if lsidIdx >= 0 {
				lsid, _, err := GetAsBSON(cmd[lsidIdx])
				if err != nil {
					return nil, nil, fmt.Errorf("error retrieving lsid %v", err)
				}
				txnNumberIdx := BSONIndexOf(cmd, "txnNumber")
				if txnNumberIdx >= 0 {
					txnNumber, ok := cmd[txnNumberIdx].Value.(int64)
					if !ok {
						return nil, nil, fmt.Errorf("txnNumber was not of type int64")
					}
					s.logger.Logf(slogger.DEBUG, "Looking up session by transaction %v %v/%v", SSLServerName, lsid, txnNumber)
					worker, found := s.LookupSessionByTransaction(SSLServerName, lsid, txnNumber)
					if !found {
						return nil, m, NewMongoError(fmt.Errorf("Given transaction number %v does not match any in-progress transactions. The active transaction number is -1", txnNumber), TransactionNotFoundErrorCode, "NoSuchTransaction")
					}
					s.logger.Logf(slogger.DEBUG, "Session found with transaction %v %v/%v", SSLServerName, lsid, txnNumber)
					return worker, m, nil
				}
			}
		}

		// check for a cursor id
		getMoreIdx := BSONIndexOf(cmd, "getMore")
		if getMoreIdx >= 0 {
			var ok bool
			cursorId, ok := cmd[getMoreIdx].Value.(int64)
			if !ok {
				return nil, nil, fmt.Errorf("getMore's cursor id was not of type int64")
			}
			s.logger.Logf(slogger.DEBUG, "Looking up session by cursorId %v/%v", SSLServerName, cursorId)
			worker, found := s.LookupSessionByCursorId(SSLServerName, cursorId)
			if !found {
				return nil, m, NewMongoError(fmt.Errorf("cursor id %v not found", cursorId), CursorNotFoundErrorCode, "CursorNotFound")
			}
			s.logger.Logf(slogger.DEBUG, "Session found with cursorId %v/%v", SSLServerName, cursorId)
			return worker, m, nil
		}
	default:
		s.logger.Logf(slogger.DEBUG, "Skipping lookupPinnedWorker for message not of type MessageMessage (was %T)", message)
	}
	return nil, m, nil
}
