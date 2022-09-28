package shardgrpc

import (
	"context"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/teng231/shardgrpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func TestCheckShardKey(t *testing.T) {
	arr := []string{"a", "b"}
	_, index := GetShardAddressFromShardKey("thanh", arr)
	log.Print(index)
}

type TestCacheApiServer struct {
	ncall int
}

type TestShardApiServer struct {
	id     int
	shards []string
}

func (me *TestShardApiServer) ListVisitors(ctx context.Context, req *pb.VisitorRequest) (*pb.Visitors, error) {
	// log.Print(ctx)
	return &pb.Visitors{Total: 10}, nil
}

func (me TestShardApiServer) Serve(id int) {
	lis, err := net.Listen("tcp", me.shards[id])
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor(me.shards, id)))
	pb.RegisterVistorServiceServer(grpcServer, &me)
	grpcServer.Serve(lis)
}

func TestShardServer(t *testing.T) {
	server0 := &TestShardApiServer{shards: []string{":21240", ":21241"}}
	go server0.Serve(0)

	server1 := &TestShardApiServer{shards: []string{":21240", ":21241"}}
	go server1.Serve(1)

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

func TestShardServerInconsistent(t *testing.T) {
	server0 := &TestShardApiServer{shards: []string{":21260", ":21241"}}
	go server0.Serve(0)

	server1 := &TestShardApiServer{shards: []string{":21260"}}
	go server1.Serve(1)

	conn, err := grpc.Dial(":21260",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(UnaryClientInterceptor()))
	if err != nil {
		panic(err)
	}
	client := pb.NewVistorServiceClient(conn)

	// server 0 => proxy to server 1 => proxy to server 0
	// must redirect
	var header2 metadata.MD // variable to store header and trailer
	client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh1"}, grpc.Header(&header2))
	if strings.Join(header2.Get("total_shards"), "") != "2" {
		t.Fatal("SHOULD RETURN SHARD NUM", strings.Join(header2.Get("total_shards"), ""))
	}
	// must exit, should not loop forever
}

func TestShardServerAndClient(t *testing.T) {
	server0 := &TestShardApiServer{shards: []string{":21250", ":21251"}}
	go server0.Serve(0)

	server1 := &TestShardApiServer{shards: []string{":21250", ":21251"}}
	go server1.Serve(1)

	conn, err := grpc.Dial(":21250",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(UnaryClientInterceptor()))
	if err != nil {
		panic(err)
	}
	client := pb.NewVistorServiceClient(conn)
	time.Sleep(100 * time.Millisecond)
	// correct server
	var header metadata.MD // variable to store header and trailer
	resp, err := client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh"}, grpc.Header(&header))
	log.Print("header", header)
	log.Print(resp, err)
	log.Print("------------------------------------------------------------------------------------------------")
	// must redirect
	var header2 metadata.MD // variable to store header and trailer
	resp, err = client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh1"}, grpc.Header(&header2))
	log.Print("header2: ", header2)
	log.Print(resp, err)
	if strings.Join(header2.Get("shard_addrs"), "") != ":21250:21251" {
		t.Fatal("SHOULD REDIRECT", strings.Join(header2.Get("shard_addrs"), ""))
	}

	// we have learn about the servers, should not redirect agan
	var header3 metadata.MD // variable to store header and trailer
	client.ListVisitors(context.Background(), &pb.VisitorRequest{AccountId: "thanh1"}, grpc.Header(&header3))
	if strings.Join(header3.Get("total_shards"), "") != "" {
		t.Fatal("SHOULD NOT REDIRECT")
	}
}
