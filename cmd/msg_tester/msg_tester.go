package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
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
		log.Fatal("Connection", err)
	}
	return conn
}

func send(conn driver.Connection, b []byte) error {
	goctx := context.TODO()
	if err := conn.WriteWireMessage(goctx, b); err != nil {
		return err
	}

	ret, err := conn.ReadWireMessage(goctx)
	if err != nil {
		return err
	}
	resp, err := mongonet.ReadMessageFromBytes(ret)
	if err != nil {
		return err
	}
	if mm, ok := resp.(*mongonet.MessageMessage); ok {
		for _, sec := range mm.Sections {
			if bodySection, ok := sec.(*mongonet.BodySection); ok && bodySection != nil {
				res, err := bson.Raw(bodySection.Body.BSON).LookupErr("ok")
				if err != nil {
					return err
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

func MD5Bytes(data []byte) []byte {
	checksum := md5.Sum(data)
	return checksum[:]
}

func longSideNativeCli(client *mongo.Client) {
	log.Println("longSideNativeCli")
	var res bson.Raw
	coll := client.Database("accounts").Collection("users")
	err := coll.FindOne(context.TODO(),
		bson.M{"pop": 444},
	).Decode(&res)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res, MD5Bytes(res))
	firstUpdate := bson.D{
		{"$set", bson.D{{"_crc", MD5Bytes(res)}}}}
	//	firstUpdate := bson.D{
	//		{"$set", bson.E{"_crc", MD5Bytes(res)}}}
	fmt.Println(firstUpdate)
	req := mongo.NewUpdateOneModel().SetFilter(bson.M{"pop": "444"}).SetUpdate(firstUpdate).SetUpsert(true)
	//req := mongo.NewReplaceOneModel().SetFilter(mFilter).SetReplacement(firstUpdate).SetUpsert(true)
	opts2 := options.BulkWrite().SetOrdered(false)
	_, err = coll.BulkWrite(context.TODO(), []mongo.WriteModel{req}, opts2)
	if err != nil {
		log.Fatal("xx", err)
	}
}

func oneConn(conn driver.Connection, client *mongo.Client) {
	i := 0
	for {
		fmt.Println("-------------------------------------------", i, conn.Address(), conn.ID())
		if i > 15 {
			break
		}
		key := fmt.Sprintf("user%d", i)
		firstUpdate := bson.D{{"$set", bson.D{{"pop2", 77777}}}}

		item := bson.M{
			"u":      firstUpdate,
			"upsert": true,
			"q": bson.D{bson.E{Key: "$and", Value: bson.A{
				bson.D{{"username", key}},
				bson.D{{"pop", 44455555555}},
			}}},
		}
		item1 := bson.M{
			"u":      firstUpdate,
			"upsert": true,
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

		//longSideNativeCli(client)
		i++
		time.Sleep(2 * time.Second)
		conn.Close()
	}
	//conn.Close()
}
func testlongconn(client *mongo.Client) {
	conn := getConn(client)
	defer conn.Close()
	//longSideNativeCli(client)

	//return
	oneConn(conn, client)

	select {}
	//client.Disconnect(context.TODO())
	select {}
	log.Println("try native client again....")
	longSideNativeCli(client)
	longSideNativeCli(client)
	longSideNativeCli(client)
	select {}
}

func ParseValue(result interface{}) error {
	uu := bson.D{{"f", bson.A{"ss"}}}
	uur, err2 := bson.Marshal(uu)
	if err2 != nil {
		log.Fatal("ss", err2)
	}
	uu2 := bson.D{{"kk", bson.A{bson.D{{"f1", "v11"}}}}}
	uur2, _ := bson.Marshal(uu2)
	cmd := bson.D{
		{"update", "users"},
		{"updates", uur},
		{"ordered", false},
		{"$db", "accounts"},
	}
	cmd2 := bson.D{
		{"update", "users2"},
		{"updates", uur2},
		{"ordered", true},
		{"$db", "accounwwts"},
	}
	fin := bson.D{{"s1", cmd}, {"s2", cmd2}}
	if raw, err := bson.Marshal(fin); err != nil {
		return err
	} else {
		if err := bson.Unmarshal(raw, result); err != nil {
			return err
		}

	}
	return nil
}

type dbt string

type upModel struct {
	Update  string `bson:"update"`
	Updates []byte `bson:"updates"`
	Order   bool   `bson:"ordered"`
	DB      dbt    `bson:"$db"`
}

type kv struct {
	F string `bson:"f1"`
}

type ssb struct {
	A  int    `bson:"a,omitempty"`
	AA string `bson:"aa,omitempty"`
}

type sbb struct {
	B   string
	SSB ssb `bson:"ssbs"`
}

type ABC struct {
	A int    `bson:"a,omitempty"`
	B string `bson:"b"`
	C []int  `bson:"c,omitempty"`
}

type inABC struct {
	ABC
}

func ttsbb() {
	ia := ABC{B: "33"}
	raw, err := bson.Marshal(&ia)
	if err != nil {
		log.Fatal(err)
	}
	var ia2 ABC
	err = bson.Unmarshal(raw, &ia2)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ia2)
	return
	so := sbb{B: "2", SSB: ssb{A: 33}}
	raw, err = bson.Marshal(&so)
	if err != nil {
		log.Fatal(err)
	}
	var soo sbb
	err = bson.Unmarshal(raw, &soo)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(soo, soo.SSB)
	os.Exit(0)
}

func testBsonOp() {
	ttsbb()
	oo := make(map[string]*upModel)
	if err := ParseValue(&oo); err != nil {
		log.Fatal(err)
	}
	fmt.Println(oo["s1"], oo["s2"])
	fmt.Printf("%T", oo["s1"].Updates)
	var result map[string][]*kv
	bson.Unmarshal(oo["s2"].Updates, &result)
	fmt.Println("ss", result["kk"][0])
	return
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

var host = "127.0.0.1"
var port = "27017"
var mongoAcc string

func init() {
	flag.StringVar(&host, "host", host, "host")
	flag.StringVar(&port, "port", port, "port")
	flag.StringVar(&mongoAcc, "mongoAcc", mongoAcc, "mongoAcc")
}

func decode(raw []byte) {
	resp, err := mongonet.ReadMessageFromBytes(raw)
	if err != nil {
		log.Fatal(err)
	}
	var msg *mongonet.MessageMessage
	var ok bool
	if msg, ok = resp.(*mongonet.MessageMessage); !ok {
		err := fmt.Errorf("not mongonet.MessageMessage")
		log.Fatal(err)
	}
	var sb *mongonet.SimpleBSON
	for _, sec := range msg.Sections {
		if bodySection, ok := sec.(*mongonet.BodySection); ok && bodySection != nil {
			sb = &bodySection.Body
			break
		}
	}
	cmd, err := mongonet.SimpleBsonToBSOND(sb)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cmd)
}
func testCodec() {
	key := "sss"
	firstUpdate := bson.D{{"$set", bson.D{{"pop2", 77777}}}}
	item := bson.M{
		"u":      firstUpdate,
		"upsert": true,
		"q": bson.D{bson.E{Key: "$and", Value: bson.A{
			bson.D{{"username", key}},
			bson.D{{"pop", 44455555555}},
		}}},
	}
	item1 := bson.M{
		"u":      firstUpdate,
		"upsert": true,
		"q": bson.D{bson.E{Key: "$and", Value: bson.A{
			bson.D{{"username", key}},
			bson.D{{"pop", 444}},
		}}},
	}
	cmd := bson.D{
		{"update", "users"},
		{"updates", []interface{}{item, item1}},
		{"ordered", false},
		{"writeConcern", bson.M{"w": "majority"}},
		{"$db", "accounts"},
	}
	fmt.Println("ori", cmd)
	raw := encode(cmd)
	decode(raw)
}
func main() {
	flag.Parse()
	testCodec()
	return
	uri := fmt.Sprintf("mongodb://%s:%s/?replicaSet=rs0", host, port)
	clientOptions := options.Client().ApplyURI(uri)
	clientOptions.SetMaxPoolSize(10)
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.W(1), writeconcern.J(false)))
	if mongoAcc != "" {
		fvals := strings.Split(mongoAcc, ":")
		user := fvals[0]
		password := fvals[1]
		credential := options.Credential{Username: user, Password: password}
		clientOptions.SetAuth(credential)
	}
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	testlongconn(client)
	return
	//cmd := bson.D{{"ping", 1}, {"$db", "admin"}}
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

		response, err := mongonet.RunCommandUsingRawBSON(cmd, client, context.TODO())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(response)
	*/
}
