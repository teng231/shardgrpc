package shardgrpc

import (
	"context"
	"log"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/teng231/shardgrpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// type TestCacheApiServer struct {
// 	ncall int
// }

// type TestShardApiServer struct {
// 	id     int
// 	shards []string
// }

func (me TestShardApiServer) ServeV2(id int) {
	lis, err := net.Listen("tcp", me.shards[id])
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptorV2(me.shards, id)))
	pb.RegisterVistorServiceServer(grpcServer, &me)
	grpcServer.Serve(lis)
}

func (me TestShardApiServer) ServeV0(id int) {
	lis, err := net.Listen("tcp", me.shards[id])
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterVistorServiceServer(grpcServer, &me)
	grpcServer.Serve(lis)
}

// clientshard -> servershard
func TestShardServerAndClientv2(t *testing.T) {
	server0 := &TestShardApiServer{shards: []string{":21250", ":21251"}}
	go server0.ServeV2(0)

	server1 := &TestShardApiServer{shards: []string{":21250", ":21251"}}
	go server1.ServeV2(1)

	conn, err := grpc.Dial(":21251",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(UnaryClientInterceptorV2()))
	if err != nil {
		panic(err)
	}
	client := pb.NewVistorServiceClient(conn)
	time.Sleep(2 * time.Second)
	// correct server
	var header metadata.MD // variable to store header and trailer
	resp, err := client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh"}, grpc.Header(&header))
	log.Print("header", header)
	log.Print(resp, err)
	if resp.Total == 0 {
		t.Fail()
	}
	log.Print("------------------------------------------------------------------------------------------------")
	// must redirect
	var header2 metadata.MD // variable to store header and trailer
	resp, err = client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh1"}, grpc.Header(&header2))
	log.Print(resp, err)
	if strings.Join(header2.Get("shard_addrs"), "") != ":21250:21251" {
		t.Fatal("SHOULD REDIRECT", strings.Join(header2.Get("shard_addrs"), ""))
	}
	if resp.Total == 0 {
		t.Fail()
	}
	log.Print("------------------------------------------------------------------------------------------------")
	// we have learn about the servers, should not redirect agan
	var header3 metadata.MD // variable to store header and trailer
	resp, err = client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh"}, grpc.Header(&header3))
	log.Print(resp, err)
	// if strings.Join(header3.Get("total_shards"), "") != "" {
	// 	t.Fatal("SHOULD NOT REDIRECT")
	// }
	log.Print("------------------------------------------------------------------------------------------------")
	// correct server
	var header4 metadata.MD // variable to store header and trailer
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, shard_key, "thanh1")
	resp, err = client.ListVisitors(ctx, &pb.VisitorRequest{}, grpc.Header(&header4))
	log.Print("headerxxxxx", header4)
	log.Print(resp, err)
	if resp.Total == 0 {
		t.Fail()
	}
}

// clientshard -> normal server
func TestNotShardServerV2(t *testing.T) {
	server0 := &TestShardApiServer{shards: []string{":21240", ":21241"}}
	go server0.ServeV0(0)

	server1 := &TestShardApiServer{shards: []string{":21240", ":21241"}}
	go server1.ServeV0(1)

	conn, err := grpc.Dial(":21240",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(UnaryClientInterceptorV2()))
	if err != nil {
		panic(err)
	}
	client := pb.NewVistorServiceClient(conn)

	// correct server
	var header metadata.MD // variable to store header and trailer
	data, err := client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh"}, grpc.Header(&header))
	log.Print(data, err, header)
	// if strings.Join(header.Get("shard_addrs"), "") != "" {
	// 	t.Fatal("SHOULD NOT RETURN ANY SHARD_NUM")
	// }

	// must redirect
	var header2 metadata.MD // variable to store header and trailer
	data, err = client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh1"}, grpc.Header(&header2))
	log.Print(data, err, header2)
	// if strings.Join(header2.Get("shard_addrs"), ",") != ":21240,:21241" {

	// 	t.Fatal("SHOULD RETURN SHARD NUM", strings.Join(header2.Get("shard_addrs"), ","))
	// }
}

// normal client -> servershard
func TestNotShardServerV2WithNormalcase(t *testing.T) {
	server0 := &TestShardApiServer{shards: []string{":21240", ":21241"}}
	go server0.ServeV2(0)

	server1 := &TestShardApiServer{shards: []string{":21240", ":21241"}}
	go server1.ServeV2(1)

	conn, err := grpc.Dial(":21240", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	client := pb.NewVistorServiceClient(conn)

	// correct server
	var header metadata.MD // variable to store header and trailer
	data, err := client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh"}, grpc.Header(&header))
	log.Print(data, err, header)
	// if strings.Join(header.Get("shard_addrs"), "") != "" {
	// 	t.Fatal("SHOULD NOT RETURN ANY SHARD_NUM")
	// }

	// must redirect
	var header2 metadata.MD // variable to store header and trailer
	data, err = client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh1"}, grpc.Header(&header2))
	log.Print(data, err, header2)
	// if strings.Join(header2.Get("shard_addrs"), ",") != ":21240,:21241" {

	// 	t.Fatal("SHOULD RETURN SHARD NUM", strings.Join(header2.Get("shard_addrs"), ","))
	// }
}

func TestResolveHost(t *testing.T) {
	hostname := "a12-service-1"
	arr := strings.Split(hostname, "-")
	if len(arr) < 2 {
		log.Panicf("hostname '%s' not valid form xxx-i", hostname)
	}
	index, err := strconv.Atoi(arr[len(arr)-1])
	log.Print(index, err)
}
