package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"
	"unsafe"

	"github.com/mongodb/mongonet"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

func encode(cmd interface{}) []byte {
	sb, err := mongonet.SimpleBSONConvert(cmd)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(bson.Raw(sb.BSON).String())
	newmsg := &mongonet.MessageMessage{
		mongonet.MessageHeader{
			0,
			17,
			1,
			mongonet.OP_MSG},
		0,
		[]mongonet.MessageMessageSection{
			&mongonet.BodySection{
				sb,
			},
		},
	}
	return newmsg.Serialize()
}
func mainq(client *mongo.Client, command interface{}) {
	var db *mongo.Database
	db = client.Database("accounts")
	// Run an explain command to see the query plan for when a "find" is
	// executed on collection "bar" specify the ReadPreference option to
	// explicitly set the read preference to primary.
	opts := options.RunCmd().SetReadPreference(readpref.Primary())
	var result bson.M
	err := db.RunCommand(context.TODO(), command, opts).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result)
}

func extractTopology(mc *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(mc).Elem()
	d := e.FieldByName("deployment")
	if d.IsZero() {
		log.Fatal("failed to extract deployment topology")
	}
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

func getConn(mc *mongo.Client) driver.Connection {
	tl := extractTopology(mc)
	if tl == nil {
		log.Fatal("tl == nil")
	}
	srv, err := tl.SelectServer(context.TODO(), description.ReadPrefSelector(readpref.Primary()))
	if err != nil {
		log.Fatal(err)
	}
	conn, err := srv.Connection(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func send(conn driver.Connection, b []byte) error {
	goctx := context.TODO()
	if err := conn.WriteWireMessage(goctx, b); err != nil {
		log.Fatal(err)
	}

	ret, err := conn.ReadWireMessage(goctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	resp, err := mongonet.ReadMessageFromBytes(ret)
	if err != nil {
		log.Fatal(err)
	}
	if mm, ok := resp.(*mongonet.MessageMessage); ok {
		for _, sec := range mm.Sections {
			if bodySection, ok := sec.(*mongonet.BodySection); ok && bodySection != nil {
				res, err := bson.Raw(bodySection.Body.BSON).LookupErr("ok")
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(res)
				return nil
				/*
					respBsonD, err := bodySection.Body.ToBSOND()
					if err != nil {
						log.Fatal(err)
					}
					respBsonD.Map()
					for i := range respBsonD {
						fmt.Println(respBsonD[i].Key, respBsonD[i].Value)
						if respBsonD[i].Key == "ok" {
							if int(respBsonD[i].Value.(float64)) == 0 {
								return fmt.Errorf("fail")
							}
						}
					}
					fmt.Println("res", respBsonD)
					return nil
				*/
			}
		}
	} else {
		log.Fatal(fmt.Errorf("expected an OP_MSG response but got %T", resp))
	}
	log.Fatal(fmt.Errorf("couldn't find a body section in response"))
	return nil
}

func testlongconn(client *mongo.Client) {
	conn := getConn(client)
	defer conn.Close()
	i := 0
	for {
		fmt.Println("-------------------------------------------", i)
		if i > 5 {
			i = 0
		}
		key := fmt.Sprintf("user%d", i)
		firstUpdate := bson.D{{"$set", bson.D{{"pop2", 77777}}}}

		item := bson.M{
			"u":      firstUpdate,
			"upsert": false,
			"q": bson.D{bson.E{Key: "$and", Value: bson.A{
				bson.D{{"username", key}},
				bson.D{{"pop", 44455555555}},
			}}},
		}
		item1 := bson.M{
			"u":      firstUpdate,
			"upsert": false,
			"q": bson.D{bson.E{Key: "$and", Value: bson.A{
				bson.D{{"username", key}},
				bson.D{{"pop", 444}},
			}}},
		}
		/*
				item := bson.D{
					//{"q", bson.D{{"username", key}, {"pop", 444}}},
					{"q", bson.D{bson.E{Key: "$and", Value: bson.A{
						bson.D{{"username", key}},
						bson.D{{"pop", 44455555555}},
					}}}},
					{"u", firstUpdate},
					{"upsert", false},
				}



			cmd := bson.M{
				"update":  "users",
				"updates": []interface{}{item},
				"ordered": false,
				"$db":     "accounts",
			}
		*/
		cmd := bson.D{

			{"update", "users"},
			{"updates", []interface{}{item, item1}},
			{"ordered", false},
			{"writeConcern", bson.M{"w": "majority"}},
			{"$db", "accounts"},
		}
		err := send(conn, encode(cmd))
		if err != nil {
			fmt.Println("getconn again")
			conn.Close()
			conn = getConn(client)
		}
		i++
		time.Sleep(2 * time.Second)
	}
}

func testBsonOp() {
	key := "user02"

	firstUpdate := bson.D{{"$set", bson.D{{"pop2", 41334}}}}
	others := bson.D{
		{"ordered", false},
		{"writeConcern", bson.M{"w": "majority"}},
	}
	firstUpdate = append(firstUpdate, others...)
	fmt.Println(firstUpdate)
	return
	/*item := bson.D{
		//{"q", bson.D{{"username", key}, {"pop", 444}}},
		{"q", bson.D{bson.E{Key: "$and", Value: bson.A{
			bson.D{{"username", key}},
			bson.D{{"pop", 444}},
		}}}},
		{"u", firstUpdate},
		{"upsert", false},
	}*/
	item := bson.M{
		//{"q", bson.D{{"username", key}, {"pop", 444}}},
		"q": bson.D{bson.E{Key: "$and", Value: bson.A{
			bson.D{{"username", key}},
			bson.D{{"pop", 444}},
		}}},
		"u":      firstUpdate,
		"upsert": false,
	}

	cmd := bson.D{
		{"update", "users"},
		{"updates", []interface{}{item}},
		{"ordered", false},
		{"$db", "accounts"},
	}
	sb, err := mongonet.SimpleBSONConvert(cmd)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(bson.Raw(sb.BSON).String())
	res, err := bson.Raw(sb.BSON).LookupErr("updates")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(bson.Raw(res.Array().Index(0).Value().Value).Lookup("u"))
}

func main() {
	testBsonOp()
	return
	host := "127.0.0.1"
	port := "27017"
	uri := fmt.Sprintf("mongodb://%s:%s/?replicaSet=rs0", host, port)
	clientOptions := options.Client().ApplyURI(uri)
	clientOptions.SetMaxPoolSize(10)
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.W(1), writeconcern.J(false)))
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	testlongconn(client)
	return
	cmd := bson.D{{"ping", 1}, {"$db", "admin"}}
	//key := "user02"
	/*
		firstUpdate := bson.D{{"$set", bson.D{{"pop2", 41334}}}}
		item := bson.D{
			//{"q", bson.D{{"username", key}, {"pop", 444}}},
			{"q", bson.D{bson.E{Key: "$and", Value: bson.A{
				bson.D{{"username", key}},
				bson.D{{"pop", 444}},
			}}}},
			{"u", firstUpdate},
			{"upsert", false},
		}

		cmd := bson.D{
			{"update", "users"},
			{"updates", []interface{}{item}},
			{"ordered", false},
			//{"$db", "accounts"},
		}
		//req := encode(cmd)
		//mainq(client, cmd)
		//return
	*/
	response, err := mongonet.RunCommandUsingRawBSON(cmd, client, context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(response)
}
