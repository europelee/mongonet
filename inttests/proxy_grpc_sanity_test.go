package inttests

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	. "github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestProxyMongosModeGRPCSanity(t *testing.T) {
	mongoPort, proxyPort, hostname := util.GetTestHostAndPorts()
	t.Logf("using proxy port %v", proxyPort)
	pc := getProxyConfig(hostname, mongoPort, proxyPort, DefaultMaxPoolSize, DefaultMaxPoolIdleTimeSec, util.Cluster, false, nil)
	pc.GRPCServerConfig = GRPCServerConfig{
		Enabled:  true,
		BindHost: hostname,
		BindPort: proxyPort + 1,
	}
	pc.LogLevel = slogger.DEBUG
	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	go proxy.Run()

	// Set up a connection to the server.
	goctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	grpcHostPort := fmt.Sprintf("%v:%v", hostname, proxyPort+1)
	conn, err := grpc.DialContext(goctx, grpcHostPort, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	insert := bson.D{
		{"insert", "foo"},
		{"$db", "test"},
		{"documents", bson.A{
			bson.D{{"_id", 42}},
		}},
	}
	err = sendAndRecvMsg(t, conn, insert)
	if err != nil {
		t.Error(err)
	}

	find := bson.D{
		{"find", "foo"},
		{"$db", "test"},
		{"filter", bson.D{}},
	}
	err = sendAndRecvMsg(t, conn, find)
	if err != nil {
		t.Error(err)
	}
}

func sendAndRecvMsg(t *testing.T, conn *grpc.ClientConn, bson bson.D) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "ServerName", "TestServerName")
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{
		StreamName:    "Send",
		ServerStreams: true,
	}, "/mongonet/Send", grpc.ForceCodec(BytesCodec{}))
	if err != nil {
		return err
	}

	t.Logf("Sending gRPC msg: %v", bson)
	doc, err := SimpleBSONConvert(bson)
	if err != nil {
		return err
	}
	cmd := &MessageMessage{
		MessageHeader{
			0,
			17,
			0,
			OP_MSG},
		0,
		[]MessageMessageSection{
			&BodySection{
				doc,
			},
		},
	}

	// send
	in := cmd.Serialize()
	if err := stream.SendMsg(&in); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}

	// receive
	var trailer metadata.MD
	for {
		var out []byte
		err := stream.RecvMsg(&out)
		if err == io.EOF {
			trailer = stream.Trailer()
			break
		}
		if err != nil {
			return err
		}
		m, err := ReadMessageFromBytes(out)
		if err != nil {
			return err
		}
		mm, ok := m.(*MessageMessage)
		if !ok {
			return fmt.Errorf("Unable to convert to MessageMessage")
		}
		msg, _, err := MessageMessageToBSOND(mm)
		if err != nil {
			return err
		}
		t.Logf("Received gRPC response: %v", msg)

		// check response for { ok: 1 }
		idx := BSONIndexOf(msg, "ok")
		if idx < 0 {
			return fmt.Errorf("can't find ok in response")
		}
		ok, _, err = GetAsBool(msg[idx])
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("Command did not return ok")
		}
	}
	if trailer != nil {
		t.Logf("Received trailer: %v", trailer)
	}
	return nil
}

func TestProxyMongosModeGRPCTransaction(t *testing.T) {
	mongoPort, proxyPort, hostname := util.GetTestHostAndPorts()
	t.Logf("using proxy port %v", proxyPort)
	pc := getProxyConfig(hostname, mongoPort, proxyPort, DefaultMaxPoolSize, DefaultMaxPoolIdleTimeSec, util.Cluster, false, nil)
	pc.GRPCServerConfig = GRPCServerConfig{
		Enabled:  true,
		BindHost: hostname,
		BindPort: proxyPort + 1,
	}
	pc.LogLevel = slogger.DEBUG
	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	go proxy.Run()

	// Set up a connection to the server.
	goctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	grpcHostPort := fmt.Sprintf("%v:%v", hostname, proxyPort+1)
	t.Logf("connecting to %v", grpcHostPort)
	conn, err := grpc.DialContext(goctx, grpcHostPort, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	id := primitive.Binary{
		Subtype: uint8(4),
		Data:    []byte("blahblahblahblah"),
	}
	insert := bson.D{
		{"insert", "foo"},
		{"$db", "test"},
		{"startTransaction", true},
		{"autocommit", false},
		{"txnNumber", int64(1)},
		{"lsid", bson.D{
			{"id", id},
		}},
		{"documents", bson.A{
			bson.D{{"_id", 43}},
		}},
	}
	err = sendAndRecvMsg(t, conn, insert)
	if err != nil {
		t.Error(err)
	}

	find := bson.D{
		{"find", "foo"},
		{"$db", "test"},
		{"autocommit", false},
		{"txnNumber", int64(1)},
		{"lsid", bson.D{
			{"id", id},
		}},
		{"filter", bson.D{}},
	}
	err = sendAndRecvMsg(t, conn, find)
	if err != nil {
		t.Error(err)
	}

	commitTransaction := bson.D{
		{"commitTransaction", 1},
		{"$db", "admin"},
		{"autocommit", false},
		{"txnNumber", int64(1)},
		{"lsid", bson.D{
			{"id", id},
		}},
	}
	err = sendAndRecvMsg(t, conn, commitTransaction)
	if err != nil {
		t.Error(err)
	}
}

func TestProxyMongosModeGRPCCursor(t *testing.T) {
	mongoPort, proxyPort, hostname := util.GetTestHostAndPorts()
	t.Logf("using proxy port %v", proxyPort)
	pc := getProxyConfig(hostname, mongoPort, proxyPort, DefaultMaxPoolSize, DefaultMaxPoolIdleTimeSec, util.Cluster, false, nil)
	pc.GRPCServerConfig = GRPCServerConfig{
		Enabled:  true,
		BindHost: hostname,
		BindPort: proxyPort + 1,
	}
	pc.LogLevel = slogger.DEBUG
	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	go proxy.Run()

	// Set up a connection to the server.
	goctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	grpcHostPort := fmt.Sprintf("%v:%v", hostname, proxyPort+1)
	t.Logf("connecting to %v", grpcHostPort)
	conn, err := grpc.DialContext(goctx, grpcHostPort, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	insert := bson.D{
		{"insert", "foo"},
		{"$db", "test"},
		{"documents", bson.A{
			bson.D{{"_id", 44}},
			bson.D{{"_id", 45}},
			bson.D{{"_id", 46}},
			bson.D{{"_id", 47}},
			bson.D{{"_id", 48}},
			bson.D{{"_id", 49}},
		}},
	}
	err = sendAndRecvMsg(t, conn, insert)
	if err != nil {
		t.Error(err)
	}

	find := bson.D{
		{"find", "foo"},
		{"$db", "test"},
		{"filter", bson.D{}},
		{"limit", 10},
		{"batchSize", 2},
	}
	err = sendAndRecvMsg(t, conn, find)
	if err != nil {
		t.Error(err)
	}
}
