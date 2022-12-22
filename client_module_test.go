package shardgrpc

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/teng231/shardgrpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

type TestApiServer struct {
	// id     int
	shards []string
}

func (me *TestApiServer) ListVisitors(ctx context.Context, req *pb.VisitorRequest) (*pb.Visitors, error) {
	// log.Print(ctx)
	return &pb.Visitors{Total: 20}, nil
}

func (me *TestApiServer) Ping(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {
	// log.Print(ctx)
	return &pb.Empty{}, nil
}

func (me TestApiServer) Serve(id int) {
	lis, err := net.Listen("tcp", me.shards[id])
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor(me.shards, id)))
	pb.RegisterVistorServiceServer(grpcServer, &me)
	grpcServer.Serve(lis)
}

func TestTestCheckPing(t *testing.T) {

	server0 := &TestApiServer{shards: []string{":21250"}}
	go server0.Serve(0)

	sclient := CreateShardClient()

	conn, err := grpc.Dial(":21250",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(sclient.UnaryClientInterceptor(
			&DialConfig{ThrottlingDuration: 10 * time.Millisecond, MaxRetryConnect: 3},
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                10 * time.Second,
				Timeout:             20 * time.Second,
				PermitWithoutStream: true,
			}),
		)))
	if err != nil {
		panic(err)
	}
	client := pb.NewVistorServiceClient(conn)
	time.Sleep(2 * time.Second)
	log.Print("shard count: ", sclient.GetShardCount())
	var header metadata.MD // variable to store header and trailer
	c, cancel := MakeContext(20, nil)
	defer cancel()
	c = metadata.AppendToOutgoingContext(c, "s_key", "thanh")
	resp, err := client.Ping(c, &pb.Empty{}, grpc.Header(&header))
	log.Print(resp, err, header)

	log.Print("shard count: ", sclient.GetShardCount())
}
