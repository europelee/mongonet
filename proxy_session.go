package mongonet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

type ProxySession struct {
	*Session

	proxy            *Proxy
	interceptor      ProxyInterceptor
	hooks            map[string]MetricsHook
	mongoConn        *MongoConnectionWrapper
	isMetricsEnabled bool
}

type MetricsHook interface {
	StartTimer() error
	StopTimer()
	ObserveWithLabels(val float64, labels map[string]string) error
	SetGauge(val float64) error
	AddCounterGauge(val float64) error
	SubGauge(val float64) error
	IncCounterGauge() error
	DecGauge() error
}

type MetricsHookFactory interface {
	NewHook(metricName string, curryWithLabels map[string]string) (MetricsHook, error)
}

type ResponseInterceptor interface {
	InterceptMongoToClient(m Message, serverAddress address.Address, isRemote bool, retryAttemptsExhausted bool, operationType string) (Message, int64, error)
	// ProcessExecutionTime records the execution time of an operation from startTime, subtracting
	// time while execution was paused, pausedExecutionTimeMicros (i.e. while sending the message back to client)
	ProcessExecutionTime(startTime time.Time, pausedExecutionTimeMicros int64)
}

type PinnedTransactionSession struct {
	lsid      bson.D
	txnNumber int64
}

func NewPinnedTransactionSession(lsid bson.D, txnNumber int64) *PinnedTransactionSession {
	return &PinnedTransactionSession{lsid, txnNumber}
}

type ProxyInterceptor interface {
	InterceptClientToMongo(m Message, previousResult SimpleBSON) (
		newMsg Message,
		ri ResponseInterceptor,
		remoteRs string,
		pinnedAddress address.Address,
		pinnedTransactionSession *PinnedTransactionSession,
		metricOperationType string,
		err error,
	)
	Close()
	TrackRequest(MessageHeader)
	TrackResponse(MessageHeader)
	CheckConnection() error
	CheckConnectionInterval() time.Duration
}

type ProxyInterceptorFactory interface {
	// This has to be thread safe, will be called from many clients
	NewInterceptor(ps *ProxySession) (ProxyInterceptor, error)
}

// -----

func (ps *ProxySession) RemoteAddr() net.Addr {
	return ps.remoteAddr
}

func (ps *ProxySession) GetLogger() *slogger.Logger {
	return ps.logger
}

func (ps *ProxySession) ServerPort() int {
	if ps.IsTCPSession() {
		return ps.proxy.Config.TCPServerConfig.BindPort
	}

	if ps.IsGRPCSession() {
		return ps.proxy.Config.GRPCServerConfig.BindPort
	}

	return 0
}

func (ps *ProxySession) Stats() bson.D {
	return bson.D{
		{"connectionPool", bson.D{
			{"totalCreated", ps.proxy.GetConnectionsCreated()},
		},
		},
	}
}

func (ps *ProxySession) DoLoopTemp(m Message) {
	defer logPanic(ps.logger)
	var err error
	var retryError *ProxyRetryError
	var shouldRetry bool
	var retryOnRs string
	for {
		if retryError != nil {
			retryOnRs = retryError.RetryOnRs
		}
		ps.mongoConn, err = ps.doLoop(m, ps.mongoConn, retryError, retryOnRs)
		m = nil
		if err != nil {
			if ps.mongoConn != nil {
				ps.mongoConn.Close(ps)
			}
			retryError, shouldRetry = err.(*ProxyRetryError)
			if shouldRetry {
				ps.logger.Logf(slogger.WARN, "%v", retryError)
				continue
			}
			if err != io.EOF {
				ps.logger.Logf(slogger.WARN, "error doing loop: %v", err)
			}
			return
		}
		retryError = nil
	}
}

func (ps *ProxySession) Close() {
	if ps.interceptor != nil {
		ps.interceptor.Close()
	}
}

func logPanic(logger *slogger.Logger) {
	if r := recover(); r != nil {
		var stacktraces bytes.Buffer
		pprof.Lookup("goroutine").WriteTo(&stacktraces, 2)
		logger.Logf(slogger.ERROR, "Recovering from mongonet panic. error is: %v \n stack traces: %v", r, stacktraces.String())
		logger.Flush()
		panic(r)
	}
}

func getReadPrefFromOpMsg(mm *MessageMessage, logger *slogger.Logger, defaultRp *readpref.ReadPref) (rp *readpref.ReadPref, err error) {
	bodyDoc, err2 := mm.BodyDoc()
	if err != nil {
		return nil, fmt.Errorf("Error getting body of document: %v", err2)
	}

	rpVal, err2 := bodyDoc.LookupErr("$readPreference")
	if err2 != nil {
		if err2 == bsoncore.ErrElementNotFound {
			return defaultRp, nil
		}
		return nil, fmt.Errorf("got an error looking up $readPreference: %v", err)
	}
	rpDoc, ok := rpVal.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$readPreference isn't a document")
	}
	opts := make([]readpref.Option, 0, 1)
	if maxStalenessVal, err := rpDoc.LookupErr("maxStalenessSeconds"); err == nil {
		if maxStalenessSec, ok := maxStalenessVal.AsInt32OK(); ok && maxStalenessSec >= MinMaxStalenessVal {
			opts = append(opts, readpref.WithMaxStaleness(time.Duration(maxStalenessSec)*time.Second))
		} else {
			return nil, fmt.Errorf("maxStalenessSeconds %v is invalid", maxStalenessVal)
		}
	}
	if modeVal, err2 := rpDoc.LookupErr("mode"); err2 == nil {
		modeStr, ok := modeVal.StringValueOK()
		if !ok {
			return nil, fmt.Errorf("mode %v isn't a string", modeVal)
		}
		switch strings.ToLower(modeStr) {
		case "primarypreferred":
			return readpref.PrimaryPreferred(opts...), nil
		case "secondary":
			return readpref.Secondary(opts...), nil
		case "secondarypreferred":
			return readpref.SecondaryPreferred(opts...), nil
		case "nearest":
			return readpref.Nearest(opts...), nil
		case "primary":
			return defaultRp, nil
		default:
			return nil, fmt.Errorf("got unsupported read preference %v", modeStr)
		}
	} else {
		return nil, errors.New("read preference is missing the required \"mode\" field")
	}
}

func PinnedServerSelector(addr address.Address) description.ServerSelector {
	return description.ServerSelectorFunc(func(t description.Topology, candidates []description.Server) ([]description.Server, error) {
		for _, s := range candidates {
			if s.Addr == addr {
				return []description.Server{s}, nil
			}
		}
		return nil, nil
	})
}

func (ps *ProxySession) getMongoConnection(
	rp *readpref.ReadPref,
	topology *topology.Topology,
	pinnedAddress address.Address,
) (*MongoConnectionWrapper, error) {
	ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "finding a server. readpref=%v, pinnedAddress=%v", rp, pinnedAddress)
	var srvSelector description.ServerSelector

	switch {
	case len(pinnedAddress.String()) > 0:
		srvSelector = PinnedServerSelector(pinnedAddress)
	case ps.proxy.Config.ConnectionMode == util.Cluster:
		srvSelector = description.ReadPrefSelector(rp)
	default:
		// Direct
		srvSelector = PinnedServerSelector(address.Address(ps.proxy.Config.MongoAddress()))
	}

	srv, err := topology.SelectServer(ps.proxy.Context, srvSelector)
	if err != nil {
		return nil, err
	}
	ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "found a server. connecting")
	ep, ok := srv.(driver.ErrorProcessor)
	if !ok {
		return nil, fmt.Errorf("server ErrorProcessor type assertion failed")
	}
	conn, err := srv.Connection(ps.proxy.Context)
	if err != nil {
		return nil, err
	}
	ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "connected to %v", conn.Address())
	ec, ok := conn.(driver.Expirable)
	if !ok {
		return nil, fmt.Errorf("bad connection type %T", conn)
	}
	return &MongoConnectionWrapper{conn, ep, ec, false, ps.proxy.logger, ps.proxy.Config.TraceConnPool}, nil
}

func wrapNetworkError(err error) error {
	labels := []string{driver.NetworkError}
	return driver.Error{Message: err.Error(), Labels: labels, Wrapped: err}
}

func (ps *ProxySession) doLoop(m Message, mongoConn *MongoConnectionWrapper, retryError *ProxyRetryError, remoteRs string) (*MongoConnectionWrapper, error) {
	var requestDurationHook, responseDurationHook, totalDurationHook, requestErrorsHook, responseErrorsHook, dbRoundTripHook, percentageTimeSpentInProxy MetricsHook

	if ps.isMetricsEnabled {
		var ok bool
		requestDurationHook, ok = ps.hooks["requestDurationHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the request processing duration metric hook")
		}
		responseDurationHook, ok = ps.hooks["responseDurationHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the response processing duration metric hook")
		}
		totalDurationHook, ok = ps.hooks["totalDurationHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the total processing duration metric hook")
		}
		requestErrorsHook, ok = ps.hooks["requestErrorsHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the request processing errors metric hook")
		}
		responseErrorsHook, ok = ps.hooks["responseErrorsHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the response processing errors metric hook")
		}
		dbRoundTripHook, ok = ps.hooks["dbRoundTripHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the db round trip time metric hook")
		}
		percentageTimeSpentInProxy, ok = ps.hooks["percentageTimeSpentInProxy"]
		if !ok {
			return nil, fmt.Errorf("could not access the percentage time spent in proxy metric hook")
		}
	}

	// reading message from client
	var err error
	var previousRes SimpleBSON
	if m != nil {
		ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "using cached message from client")
	} else if retryError == nil {
		ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "reading message from client")
		m, err = ps.ReadMessage()
		if err != nil {
			ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "reading message from client fail %v", err)
			if ps.isMetricsEnabled {
				hookErr := requestErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
				}
			}
			if err == io.EOF {
				return mongoConn, err
			}
			return mongoConn, NewStackErrorf("got error reading from client: %v", err)
		}
	} else {
		m, err = ReadMessageFromBytes(retryError.MsgToRetry.Serialize())
		if err != nil {
			return mongoConn, err
		}
		previousRes = retryError.PreviousResult
		ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "retrying a message from client on rs=%v", retryError.RetryOnRs)
	}

	startServerSelection := time.Now()
	requestProcessingStartTime := time.Now()
	totalProxyProcessingTimeElaspsed := 0.0

	var rp *readpref.ReadPref = ps.proxy.defaultReadPref
	if ps.proxy.Config.ConnectionMode == util.Cluster {
		// only concerned about OP_MSG at this point
		mm, ok := m.(*MessageMessage)
		if ok {
			rp2, err := getReadPrefFromOpMsg(mm, ps.proxy.logger, rp)
			if err != nil {
				if ps.isMetricsEnabled {
					hookErr := requestErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
					}
				}
				return mongoConn, err
			}
			if rp2 != nil {
				rp = rp2
			}
		}
	}
	ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "got message from client")
	ps.logMessageTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, m)
	var respInter ResponseInterceptor
	var pinnedAddress address.Address
	var pinnedTransactionSession *PinnedTransactionSession

	metricOperationType := "Unknown"
	pausedExecutionTimeMicros := int64(0)
	if ps.interceptor != nil {
		ps.interceptor.TrackRequest(m.Header())
		m, respInter, remoteRs, pinnedAddress, pinnedTransactionSession, metricOperationType, err = ps.interceptor.InterceptClientToMongo(m, previousRes)
		defer func() {
			if respInter != nil {
				respInter.ProcessExecutionTime(startServerSelection, pausedExecutionTimeMicros)
			}
		}()

		if err != nil {
			if m == nil {
				if ps.isMetricsEnabled {
					hookErr := requestErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
					}
				}
				return mongoConn, err
			}
			if !m.HasResponse() {
				// we can't respond, so we just fail
				if ps.isMetricsEnabled {
					hookErr := requestErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
					}
				}
				return mongoConn, err
			}
			if respondErr := ps.RespondWithError(m, err); respondErr != nil {
				if ps.isMetricsEnabled {
					hookErr := requestErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
					}
				}
				return mongoConn, NewStackErrorf("couldn't send error response to client; original error: %v, error sending response: %v", err, respondErr)
			}
			return mongoConn, nil
		}
		if ps.IsGRPCSession() && pinnedTransactionSession != nil && !ps.IsSticky() {
			ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "pinning gRPC session for transaction = %v", pinnedTransactionSession)
			ps.grpcServer.PinSessionByTransaction(ps, ps.SSLServerName, pinnedTransactionSession)
			ps.SetSticky(true)
		}
		if m == nil {
			// already responded
			return mongoConn, nil
		}
	}

	if retryError != nil && retryError.RetryOnRs != "" {
		remoteRs = retryError.RetryOnRs
	}
	if mongoConn == nil || !mongoConn.expirableConn.Alive() {
		topology := ps.proxy.topology
		if remoteRs != "" {
			ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "getting remote connection for %s", remoteRs)
			rc, ok := ps.proxy.remoteConnections.Load(remoteRs)
			if !ok {
				ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "failed to get a new remote connection to %s as it wasn't initialized", remoteRs)
				return nil, NewStackErrorf("failed to get a new remote connection to %s as it wasn't initialized", remoteRs)
			}
			topology = rc.(*RemoteConnection).topology
		}
		mongoConn, err = ps.getMongoConnection(rp, topology, pinnedAddress)
		if err != nil {
			if ps.isMetricsEnabled {
				hookErr := requestErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
				}
			}
			ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "failed to get a new connection. err=%v", err)
			return nil, NewStackErrorf("cannot get connection to mongo %v using connection mode=%v readpref=%v remoteRs=%s", err, ps.proxy.Config.ConnectionMode, rp, remoteRs)
		}
		ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "got new connection %v using connection mode=%v readpref=%v remoteRs=%s", mongoConn.conn.ID(), ps.proxy.Config.ConnectionMode, rp, remoteRs)
	}

	serverSelectionTime := time.Since(startServerSelection).Milliseconds()

	if ps.proxy.Config.ConnectionMode == util.Cluster {
		// only concerned about OP_MSG at this point
		if mm, ok := m.(*MessageMessage); ok {
			msg, bodysec, err := MessageMessageToBSOND(mm)
			if err != nil {
				ps.proxy.logger.Logf(slogger.ERROR, "error converting OP_MSG to bson.D. err=%v", err)
				return nil, NewStackErrorf("error converting OP_MSG to bson.D. err=%v", err)
			}

			maxtimeMsIdx := BSONIndexOf(msg, "maxTimeMS")
			if maxtimeMsIdx >= 0 {
				maxtimeMs := msg[maxtimeMsIdx]
				newMaxTime := 0.0
				switch val := maxtimeMs.Value.(type) {
				case float64:
					newMaxTime = val - float64(serverSelectionTime)
				case int64:
					newMaxTime = float64(val - serverSelectionTime)
				case int32:
					newMaxTime = float64(val - int32(serverSelectionTime))
				case int:
					newMaxTime = float64(val - int(serverSelectionTime))
				default:
					return nil, ps.RespondWithError(m, NewMongoError(fmt.Errorf("unable to parse maxTimeMs value %v and type %T", val, val), 50, "MaxTimeMSExpired"))
				}
				if newMaxTime < 0 {
					return nil, ps.RespondWithError(m, NewMongoError(fmt.Errorf("operation took longer than %v ms", maxtimeMs.Value), 50, "MaxTimeMSExpired"))
				}
				maxtimeMs.Value = newMaxTime
				msg[maxtimeMsIdx] = maxtimeMs
				bodysec.Body, err = SimpleBSONConvert(msg)
				if err != nil {
					return nil, NewStackErrorf("failed to convert message after updating maxTimeMs %v", err)
				}
			}
		}
	}

	if ps.isMetricsEnabled {
		requestProcessingElaspedTime := time.Since(requestProcessingStartTime).Seconds()
		requestDurationHook.ObserveWithLabels(requestProcessingElaspedTime, map[string]string{"operation": metricOperationType})
		totalProxyProcessingTimeElaspsed += requestProcessingElaspedTime
		defer func() {
			hookErr := totalDurationHook.ObserveWithLabels(totalProxyProcessingTimeElaspsed, map[string]string{"operation": metricOperationType})
			if hookErr != nil {
				ps.proxy.logger.Logf(slogger.WARN, "failed to observe totalDurationHook %v", hookErr)
			}
		}()
	}

	// Send message to mongo
	totalDBRoundtripTime := 0.0
	dbRoundTripTimerStart := time.Now()
	err = mongoConn.conn.WriteWireMessage(ps.proxy.Context, m.Serialize())
	if ps.isMetricsEnabled {
		defer func() {
			hookErr := dbRoundTripHook.ObserveWithLabels(totalDBRoundtripTime, map[string]string{"operation": metricOperationType})
			if hookErr != nil {
				ps.proxy.logger.Logf(slogger.WARN, "failed to observe dbRoundTripHook %v", hookErr)
			}
			totalTime := (totalDBRoundtripTime + totalProxyProcessingTimeElaspsed)
			if totalTime <= 0 {
				ps.proxy.logger.Logf(slogger.WARN, "total time spent in proxy + db is <= 0 with value %v", totalTime)
			} else {
				hookErr = percentageTimeSpentInProxy.ObserveWithLabels(totalProxyProcessingTimeElaspsed/totalTime, map[string]string{"operation": metricOperationType})
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to observe percentageTimeSpentInProxy %v", hookErr)
				}
			}
		}()
	}
	if err != nil {
		if ps.isMetricsEnabled {
			hookErr := requestErrorsHook.IncCounterGauge()
			if hookErr != nil {
				ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
			}
		}
		mongoConn.ep.ProcessError(wrapNetworkError(err), mongoConn.conn)
		return mongoConn, NewStackErrorf("error writing to mongo: %v", err)
	}

	if !m.HasResponse() {
		return mongoConn, nil
	}
	defer mongoConn.Close(ps)

	inExhaustMode := m.IsExhaust()

	// log the inital time the db took to respond to WriteWireMessage
	// note this will include networking time / buffering but it should give us a good order magnitute estimate of db time
	if ps.isMetricsEnabled {
		elaspedDbRoundTripTime := time.Since(dbRoundTripTimerStart).Seconds()
		totalDBRoundtripTime += elaspedDbRoundTripTime
	}

	for {
		// Read message back from mongo
		ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "reading data from mongo conn id=%v remoteRs=%s", mongoConn.conn.ID(), remoteRs)
		dbRoundTripTimerStart = time.Now()
		ret, err := mongoConn.conn.ReadWireMessage(ps.proxy.Context, nil)
		if err != nil {
			ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "error reading wire message mongo conn id=%v remoteRs=%s. err=%v", mongoConn.conn.ID(), remoteRs, err)
			if ps.isMetricsEnabled {
				hookErr := responseErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
				}
			}
			mongoConn.ep.ProcessError(wrapNetworkError(err), mongoConn.conn)
			return nil, NewStackErrorf("error reading wire message from mongo conn id=%v remoteRs=%s. err=%v", mongoConn.conn.ID(), remoteRs, err)
		}

		responseDurationTimerStart := time.Now()
		if ps.isMetricsEnabled {
			// now track the time the db takes to respond to each subsequent message
			// note this will include networking time / buffering but it should give us a good order magnitute estimate of db time
			elaspedDbRoundTripTime := time.Since(dbRoundTripTimerStart).Seconds()
			totalDBRoundtripTime += elaspedDbRoundTripTime
		}

		ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "read data from mongo conn %v", mongoConn.conn.ID())
		resp, err := ReadMessageFromBytes(ret)
		if err != nil {
			ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "error reading message from bytes on mongo conn id=%v remoteRs=%s. err=%v", mongoConn.conn.ID(), remoteRs, err)
			if ps.isMetricsEnabled {
				hookErr := responseErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
				}
			}
			if err == io.EOF {
				return nil, err
			}
			return nil, NewStackErrorf("got error reading response from mongo %v", err)
		}
		errCode := 0
		switch mm := resp.(type) {
		case *MessageMessage:
			bodyDoc, err := mm.BodyDoc()
			if err != nil {
				return nil, fmt.Errorf("Error getting body doc: %v", err)
			}
			if err := extractError(bodyDoc); err != nil {
				driverError, ok := err.(driver.Error)
				if ok {
					errCode = int(driverError.Code)
				}
				if ps.isMetricsEnabled {
					hookErr := responseErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
					}
				}
				ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "processing error %v on mongo conn id=%v remoteRs=%s", err, mongoConn.conn.ID(), remoteRs)
				mongoConn.ep.ProcessError(err, mongoConn.conn)
			}
		case *ReplyMessage:
			if err := extractError(mm.CommandDoc()); err != nil {
				driverError, ok := err.(driver.Error)
				if ok {
					errCode = int(driverError.Code)
				}
				if ps.isMetricsEnabled {
					hookErr := responseErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
					}
				}
				ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "processing error %v on mongo conn id=%v remoteRs=%s", err, mongoConn.conn.ID(), remoteRs)
				mongoConn.ep.ProcessError(err, mongoConn.conn)
			}
		}

		retryAttemptsExhausted := false
		if retryError != nil && errCode != 0 {
			if retryError.RetryCount > 1 {
				// Retry failed, decrement retryCount and retry
				retryError.RetryCount -= 1
				return nil, retryError
			} else {
				// We use this flag to signify that a retry failed after
				// the proxy retried a particular command (RetryCount number
				// of times.)
				retryAttemptsExhausted = true
			}
		}
		var cursorId int64
		if respInter != nil {
			resp, cursorId, err = respInter.InterceptMongoToClient(resp, mongoConn.conn.Address(), remoteRs != "", retryAttemptsExhausted, metricOperationType)

			if err != nil {
				if ps.isMetricsEnabled {
					hookErr := responseErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
					}
				}
				if pre, ok := err.(*ProxyRetryError); ok {
					return nil, pre
				}
				return nil, NewStackErrorf("error intercepting message %v", err)
			} else if ps.IsGRPCSession() && pinnedTransactionSession == nil && !ps.IsSticky() && cursorId > 0 {
				ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "pinning gRPC session for cursorId = %v", cursorId)
				ps.grpcServer.PinSessionByCursorId(ps, ps.SSLServerName, cursorId)
				ps.SetSticky(true)
			}
		}

		ps.logMessageTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, resp)
		if ps.isMetricsEnabled {
			responseDurationElasped := time.Since(responseDurationTimerStart).Seconds()
			responseDurationHook.ObserveWithLabels(responseDurationElasped, map[string]string{"operation": metricOperationType})
			totalProxyProcessingTimeElaspsed += responseDurationElasped
		}

		// Send message back to user
		ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "sending back data to user from mongo conn id=%v remoteRs=%s", mongoConn.conn.ID(), remoteRs)
		startPausedTimer := time.Now()
		err = ps.SendMessage(resp)
		pausedExecutionTimeMicros += time.Now().Sub(startPausedTimer).Microseconds()
		if err != nil {
			if ps.isMetricsEnabled {
				hookErr := responseErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
				}
			}
			mongoConn.bad = true
			ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "got error sending response to client from conn id=%v remoteRs=%s. err=%v", mongoConn.conn.ID(), remoteRs, err)
			return nil, NewStackErrorf("got error sending response to client from conn %v: %v", mongoConn.conn.ID(), err)
		}
		ps.logTrace(ps.proxy.logger, ps.proxy.Config.TraceConnPool, "sent back data to user from mongo conn id=%v remoteRs=%s", mongoConn.conn.ID(), remoteRs)
		if ps.interceptor != nil {
			ps.interceptor.TrackResponse(resp.Header())
		}

		if !inExhaustMode {
			return nil, nil
		}

		switch r := resp.(type) {
		case *ReplyMessage:
			if r.CursorId == 0 {
				return nil, nil
			}
		case *MessageMessage:
			if !r.HasMoreToCome() {
				// moreToCome wasn't set - stop the loop
				return nil, nil
			}
		default:
			if ps.isMetricsEnabled {
				hookErr := responseErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
				}
			}
			return nil, NewStackErrorf("bad response type from server %T", r)
		}
	}
}

func (ps *ProxySession) logTrace(logger *slogger.Logger, trace bool, format string, args ...interface{}) {
	if trace {
		msg := fmt.Sprintf(fmt.Sprintf("client: %v - %s", ps.RemoteAddr(), format), args...)
		logger.Logf(slogger.DEBUG, msg)
	}
}

func (ps *ProxySession) logMessageTrace(logger *slogger.Logger, trace bool, m Message) {
	if trace {
		var doc bson.D
		var msg string
		var err error
		switch mm := m.(type) {
		case *MessageMessage:
			for _, section := range mm.Sections {
				if bs, ok := section.(*BodySection); ok {
					doc, err = bs.Body.ToBSOND()
					if err != nil {
						logger.Logf(slogger.WARN, "failed to convert body to Bson.D. err=%v", err)
						return
					}
					break
				}
			}
			msg = fmt.Sprintf("got OP_MSG %v", doc)
		case *QueryMessage:
			doc, err = mm.Query.ToBSOND()
			if err != nil {
				logger.Logf(slogger.WARN, "failed to convert query to Bson.D. err=%v", err)
				return
			}
			msg = fmt.Sprintf("got OP_QUERY %v", doc)
		case *ReplyMessage:
			doc, err = mm.Docs[0].ToBSOND()
			if err != nil {
				logger.Logf(slogger.WARN, "failed to convert reply doc to Bson.D. err=%v", err)
				return
			}
			msg = fmt.Sprintf("got OP_REPLY %v", doc)
		default:
			// not bothering about printing other message types
			msg = fmt.Sprintf("got another type %T", mm)
		}
		msg = fmt.Sprintf("client: %v - %s", ps.RemoteAddr(), msg)
		logger.Logf(slogger.DEBUG, msg)
	}
}

// MongoConnectionWrapper is used to wrap the driver connection so we can explicitly expire (close out) connections on certain occasions that aren't being picked up by the driver
type MongoConnectionWrapper struct {
	conn          driver.Connection
	ep            driver.ErrorProcessor
	expirableConn driver.Expirable
	bad           bool
	logger        *slogger.Logger
	trace         bool
}

func (m *MongoConnectionWrapper) Close(ps *ProxySession) {
	if m.conn == nil {
		m.logger.Logf(slogger.WARN, "attempt to close a nil mongo connection. noop")
		return
	}
	id := m.conn.ID()
	if m.bad {
		if m.expirableConn.Alive() {
			ps.logTrace(m.logger, m.trace, "closing underlying bad mongo connection %v", id)
			if err := m.expirableConn.Expire(); err != nil {
				ps.logTrace(m.logger, m.trace, "failed to expire connection. %v", err)
			}
		} else {
			ps.logTrace(m.logger, m.trace, "bad mongo connection is nil!")
		}
	}
	ps.logTrace(m.logger, m.trace, "closing mongo connection %v", id)
	if m.expirableConn.Alive() {
		if err := m.conn.Close(); err != nil {
			ps.logTrace(m.logger, m.trace, "failed to close mongo connection %v: %v", id, err)
		}
	}
}
