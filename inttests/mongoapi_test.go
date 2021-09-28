package inttests

import (
	"context"
	"testing"

	"github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestCommonRunCommandUsingRawBSON(t *testing.T) {
	mongoPort, _, hostname := util.GetTestHostAndPorts()
	goctx := context.Background()
	client, err := util.GetTestClient(hostname, mongoPort, util.Direct, false, "mongoapi_tester", goctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(goctx)

	numberOfConnections := GetNumberOfConnections(t, client, goctx)
	for i := 0; i < 5; i++ {
		cmd := bson.D{{"ping", 1}, {"$db", "admin"}}
		response, err := mongonet.RunCommandUsingRawBSON(cmd, client, goctx)
		if err != nil {
			t.Fatal(err)
		}
		raw, ok := response.Map()["ok"]
		if !ok {
			t.Fatalf("expected to find 'ok' in response %+v", raw)
		}
		v, ok := raw.(float64)
		if !ok {
			t.Fatalf("expected to that 'ok' is int but got %T", raw)
		}
		if v != 1 {
			t.Fatalf("expected to get ok:1 but got %v", v)
		}
	}
	newNumberOfConnections := GetNumberOfConnections(t, client, goctx)
	if numberOfConnections != newNumberOfConnections {
		t.Fatalf("connection leak, expected numberOfConnections to be %v, got %v", numberOfConnections, newNumberOfConnections)
	}
}

func GetNumberOfConnections(t *testing.T, client *mongo.Client, ctx context.Context) int32 {
	serverStatus, err := client.Database("admin").RunCommand(
		ctx,
		bson.D{{"serverStatus", 1}},
	).DecodeBytes()
	if err != nil {
		t.Fatalf("error sending serverStatus command to client, err = %v", err)
	}

	connections, err := serverStatus.LookupErr("connections", "current")
	if err != nil {
		t.Fatalf("error looking up connections.current in serverStatus, err = %v", err)
	}
	return connections.AsInt32()
}
