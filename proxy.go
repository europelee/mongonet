package mongonet

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

type Proxy struct {
	Config ProxyConfig

	tcpServer  *TCPServer
	gRPCServer *GRPCServer

	logger          *slogger.Logger
	MongoClient     *mongo.Client
	topology        *topology.Topology
	defaultReadPref *readpref.ReadPref

	Context            context.Context
	connectionsCreated *int64
	poolCleared        *int64

	// using a sync.Map instead of a map paired with mutex because sync.Map is optimized for cases in which the access pattern is predominant by reads
	remoteConnections *sync.Map
}

type RemoteConnection struct {
	client   *mongo.Client
	topology *topology.Topology
}

// https://jira.mongodb.org/browse/GODRIVER-1760 will add the ability to create a topology.Topology from ClientOptions
func extractTopology(mc *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(mc).Elem()
	d := e.FieldByName("deployment")
	if d.IsZero() {
		panic("failed to extract deployment topology")
	}
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

/*
Clients estimate secondaries’ staleness by periodically checking the latest write date of each replica set member.
Since these checks are infrequent, the staleness estimate is coarse.
Thus, clients cannot enforce a maxStalenessSeconds value of less than 90 seconds.
https://docs.mongodb.com/manual/core/read-preference-staleness/
*/
const MinMaxStalenessVal int32 = 90

func NewProxy(pc ProxyConfig) (*Proxy, error) {
	return NewProxyWithContext(pc, context.Background())
}

func NewProxyWithContext(pc ProxyConfig, ctx context.Context) (*Proxy, error) {
	var initCount, initPoolCleared int64 = 0, 0
	defaultReadPref := readpref.Primary()
	p := &Proxy{pc, nil, nil, nil, nil, nil, defaultReadPref, ctx, &initCount, &initPoolCleared, &sync.Map{}}
	mongoClient, err := getMongoClientFromProxyConfig(p, pc, ctx)
	if err != nil {
		return nil, NewStackErrorf("error getting driver client for %v: %v", pc.MongoAddress(), err)
	}
	p.MongoClient = mongoClient
	p.topology = extractTopology(mongoClient)

	p.logger = NewLogger("proxy", pc.LogLevel, pc.Appenders)

	return p, nil
}

func getBaseClientOptions(p *Proxy, uri, appName string, trace bool, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs int) *options.ClientOptions {
	opts := options.Client()
	opts.ApplyURI(uri).
		SetAppName(appName).
		SetPoolMonitor(&event.PoolMonitor{
			Event: func(evt *event.PoolEvent) {
				switch evt.Type {
				case event.ConnectionCreated:
					if trace {
						p.logger.Logf(slogger.DEBUG, "**** Connection created %v", evt)
					}
					p.AddConnection()
				case "ConnectionCheckOutStarted":
					if trace {
						p.logger.Logf(slogger.DEBUG, "**** Connection check out started %v", evt)
					}
				case "ConnectionCheckedIn":
					if trace {
						p.logger.Logf(slogger.DEBUG, "**** Connection checked in %v", evt)
					}
				case "ConnectionCheckedOut":
					if trace {
						p.logger.Logf(slogger.DEBUG, "**** Connection checked out %v", evt)
					}
				case event.PoolCleared:
					p.IncrementPoolCleared()
				}
			},
		}).
		SetServerSelectionTimeout(time.Duration(serverSelectionTimeoutSec) * time.Second).
		SetMaxPoolSize(uint64(maxPoolSize))
	if maxPoolIdleTimeSec > 0 {
		opts.SetMaxConnIdleTime(time.Duration(maxPoolIdleTimeSec) * time.Second)
	}
	if connectionPoolHeartbeatIntervalMs > 0 {
		opts.SetHeartbeatInterval(time.Duration(connectionPoolHeartbeatIntervalMs) * time.Millisecond)
	}
	return opts
}

// should be used by remote connections
func getMongoClientFromUri(p *Proxy, uri, appName string, trace bool, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs int, rootCAs *x509.CertPool, ctx context.Context) (*mongo.Client, error) {
	opts := getBaseClientOptions(p, uri, appName, trace, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs)
	if rootCAs != nil {
		tlsConfig := &tls.Config{RootCAs: rootCAs}
		opts.SetTLSConfig(tlsConfig)
	}
	return mongo.Connect(ctx, opts)
}

// should be used by local connections
func getMongoClientFromProxyConfig(p *Proxy, pc ProxyConfig, ctx context.Context) (*mongo.Client, error) {
	var uri string
	if pc.ConnectionMode == util.Direct {
		uri = fmt.Sprintf("mongodb://%s", pc.MongoAddress())
	} else {
		uri = pc.MongoURI
	}
	opts := getBaseClientOptions(p, uri, pc.AppName, p.Config.TraceConnPool, pc.ServerSelectionTimeoutSec, pc.MaxPoolSize, pc.MaxPoolIdleTimeSec, pc.ConnectionPoolHeartbeatIntervalMs)
	opts.
		SetDirect(pc.ConnectionMode == util.Direct)

	if pc.MongoUser != "" {
		auth := options.Credential{
			AuthMechanism: "SCRAM-SHA-1",
			Username:      pc.MongoUser,
			AuthSource:    "admin",
			Password:      pc.MongoPassword,
			PasswordSet:   true,
		}
		opts.SetAuth(auth)
	}
	if pc.MongoSSL {
		tlsConfig := &tls.Config{RootCAs: pc.MongoRootCAs}
		opts.SetTLSConfig(tlsConfig)
	}
	return mongo.Connect(ctx, opts)
}

func (p *Proxy) AddRemoteConnection(rsName, uri, appName string, trace bool, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs int, rootCAs *x509.CertPool) error {
	p.logger.Logf(slogger.DEBUG, "adding remote connection for %s", rsName)
	if _, alreadyAdded := p.remoteConnections.Load(rsName); alreadyAdded {
		p.logger.Logf(slogger.DEBUG, "remote connection for %s already exists", rsName)
		return nil
	}
	client, err := getMongoClientFromUri(p, uri, appName, trace, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs, rootCAs, p.Context)
	if err != nil {
		return err
	}
	p.remoteConnections.Store(rsName, &RemoteConnection{client, extractTopology(client)})
	return nil
}

func (p *Proxy) ClearRemoteConnection(rsName string, additionalGracePeriodSec int) error {
	rc, ok := p.remoteConnections.Load(rsName)
	if !ok {
		p.logger.Logf(slogger.WARN, "remote connection for %s doesn't exist", rsName)
		return nil
	}
	p.logger.Logf(slogger.DEBUG, "clearing remote connection for %s", rsName)
	ctx2, cancelFn := context.WithTimeout(p.Context, time.Duration(additionalGracePeriodSec)*time.Second)
	defer cancelFn()
	// remote connections only has *mongo.Client, so no need for type check here. being extra safe about null clients just in case.
	if rc.(*RemoteConnection).client != nil {
		err := rc.(*RemoteConnection).client.Disconnect(ctx2)
		if err != nil {
			return err
		}
	}
	p.remoteConnections.Delete(rsName)
	p.logger.Logf(slogger.DEBUG, "remote connection %s cleared", rsName)
	return nil
}

func (p *Proxy) InitializeServer() {
	if p.Config.TCPServerConfig.Enabled {
		tcpServer := NewTCPServer(
			p.Config.TCPServerConfig,
			p,
			func(prefix string) *slogger.Logger { return NewLogger(prefix, p.Config.LogLevel, p.Config.Appenders) },
			p.Context,
		)
		p.tcpServer = &tcpServer
	}

	if p.Config.GRPCServerConfig.Enabled {
		gRPCServer := NewGRPCServer(
			p.Config.GRPCServerConfig,
			p,
			func(prefix string) *slogger.Logger { return NewLogger(prefix, p.Config.LogLevel, p.Config.Appenders) },
			p.Context,
		)
		p.gRPCServer = &gRPCServer
	}
}

func (p *Proxy) Run() error {
	var tcpErr, grpcErr error
	var wg sync.WaitGroup
	if p.tcpServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tcpErr = p.tcpServer.Run()
		}()
	}
	if p.gRPCServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			grpcErr = p.gRPCServer.Run()
		}()
	}
	wg.Wait()
	return MergeErrors(tcpErr, grpcErr)
}

// called by a synched method
func (p *Proxy) OnSSLConfig(sslPairs []*SSLPair) (ok bool, names []string, errs []error) {
	if p.tcpServer != nil {
		return p.tcpServer.OnSSLConfig(sslPairs)
	}

	ok = true
	return
}

func NewLogger(prefix string, logLevel slogger.Level, appenders []slogger.Appender) *slogger.Logger {
	filters := []slogger.TurboFilter{slogger.TurboLevelFilter(logLevel)}

	if appenders == nil {
		appenders = []slogger.Appender{slogger.StdOutAppender()}
	}

	return &slogger.Logger{prefix, appenders, 0, filters}
}

func (p *Proxy) AddConnection() {
	atomic.AddInt64(p.connectionsCreated, 1)
}

func (p *Proxy) IncrementPoolCleared() {
	atomic.AddInt64(p.poolCleared, 1)
}

func (p *Proxy) GetConnectionsCreated() int64 {
	return atomic.LoadInt64(p.connectionsCreated)
}

func (p *Proxy) GetPoolCleared() int64 {
	return atomic.LoadInt64(p.poolCleared)
}

func (p *Proxy) CreateWorker(session *Session) (ServerWorker, error) {
	var err error

	ps := &ProxySession{session, p, nil, nil, nil, false}
	if p.Config.InterceptorFactory != nil {
		ps.interceptor, err = ps.proxy.Config.InterceptorFactory.NewInterceptor(ps)
		if err != nil {
			return nil, err
		}

		if ps.proxy.Config.CollectorHookFactory != nil {
			requestDurationHook, err := ps.proxy.Config.CollectorHookFactory.NewHook("processingDuration", map[string]string{"type": "request_total"})
			if err != nil {
				return nil, err
			}

			responseDurationHook, err := ps.proxy.Config.CollectorHookFactory.NewHook("processingDuration", map[string]string{"type": "response_total"})
			if err != nil {
				return nil, err
			}

			totalDurationHook, err := ps.proxy.Config.CollectorHookFactory.NewHook("processingDuration", map[string]string{"type": "round_trip_total"})
			if err != nil {
				return nil, err
			}

			requestErrorsHook, err := ps.proxy.Config.CollectorHookFactory.NewHook("processingErrors", map[string]string{"type": "request"})
			if err != nil {
				return nil, err
			}

			responseErrorsHook, err := ps.proxy.Config.CollectorHookFactory.NewHook("processingErrors", map[string]string{"type": "response"})
			if err != nil {
				return nil, err
			}

			dbRoundTripHook, err := ps.proxy.Config.CollectorHookFactory.NewHook("dbRoundTripDuration", map[string]string{})
			if err != nil {
				return nil, err
			}

			percentageTimeSpentInProxy, err := ps.proxy.Config.CollectorHookFactory.NewHook("percentageTimeSpentInProxy", map[string]string{})
			if err != nil {
				return nil, err
			}

			ps.hooks = make(map[string]MetricsHook)
			ps.hooks["requestDurationHook"] = requestDurationHook
			ps.hooks["responseDurationHook"] = responseDurationHook
			ps.hooks["requestErrorsHook"] = requestErrorsHook
			ps.hooks["responseErrorsHook"] = responseErrorsHook
			ps.hooks["totalDurationHook"] = totalDurationHook
			ps.hooks["dbRoundTripHook"] = dbRoundTripHook
			ps.hooks["percentageTimeSpentInProxy"] = percentageTimeSpentInProxy

			ps.isMetricsEnabled = true
		}

		if session.IsTCPSession() {
			session.conn = CheckedConn{session.conn.(net.Conn), ps.interceptor}
		}
	}

	return ps, nil
}
