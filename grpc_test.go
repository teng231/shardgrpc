package shardgrpc

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/teng231/shardgrpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func TestCheckShardKey(t *testing.T) {
	arr := []string{"a", "b"}
	_, index := calcAddress("thanh", arr)
	log.Print(index)
}

func TestShard(t *testing.T) {
	arr := []string{"0", "1"}
	_, index := calcAddress("4516", arr)
	log.Print(index)
	_, index = calcAddress("4517", arr)
	log.Print(index)
}

type TestCacheApiServer struct {
	// ncall int
}

type TestShardApiServer struct {
	// id     int
	shards  []string
	servers []*ServerInfo
}

func (me *TestShardApiServer) ListVisitors(ctx context.Context, req *pb.VisitorRequest) (*pb.Visitors, error) {
	// log.Print(ctx)
	return &pb.Visitors{Total: 10}, nil
}
func (me *TestShardApiServer) Ping(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {
	// log.Print(ctx)
	return &pb.Empty{}, nil
}

func MakeContext(sec int, claims interface{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(sec)*time.Second)
	if claims != nil {
		bin, err := json.Marshal(claims)
		if err != nil {
			log.Print(err)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, "ctx", string(bin))
		return ctx, cancel
	}
	return ctx, cancel
}

// ServeV0 for testing normal server not using anything shard
func (me TestShardApiServer) ServeV0(id int) {
	lis, err := net.Listen("tcp", me.shards[id])
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterVistorServiceServer(grpcServer, &me)
	grpcServer.Serve(lis)
}

// ServeV1 for testing statefull server k8s
func (me TestShardApiServer) ServeV1(id int) {
	lis, err := net.Listen("tcp", me.shards[id])
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor(me.shards, id)))
	pb.RegisterVistorServiceServer(grpcServer, &me)
	grpcServer.Serve(lis)
}

func (me TestShardApiServer) ServeV1x(hostname, port string, shardcount int) {
	lis, err := net.Listen("tcp", hostname+":"+port)
	if err != nil {
		panic(err)
	}
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: 30 * time.Second,
		}),
		grpc.UnaryInterceptor(
			UnaryServerInterceptorStatefullset(hostname, port, "", shardcount),
		),
	}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterVistorServiceServer(grpcServer, &me)
	grpcServer.Serve(lis)
}

// ServeV2 for testing normal server using shard
func (me TestShardApiServer) ServeV2(id int) {
	ser := me.servers[id]
	lis, err := net.Listen("tcp", ser.Host+":"+strconv.Itoa(ser.Port))
	if err != nil {
		panic(err)
	}

	servers := []string{}

	for _, ser := range me.servers {
		servers = append(servers, ser.Host+":"+strconv.Itoa(ser.Port))
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor(servers, id)))
	pb.RegisterVistorServiceServer(grpcServer, &me)
	grpcServer.Serve(lis)
}

// clientshard -> servershard
func TestShardServerAndClient(t *testing.T) {
	server0 := &TestShardApiServer{servers: []*ServerInfo{{Port: 21250}, {Port: 21251}}}
	go server0.ServeV2(0)

	server1 := &TestShardApiServer{servers: []*ServerInfo{{Port: 21250}, {Port: 21251}}}
	go server1.ServeV2(1)

	time.Sleep(100 * time.Millisecond)
	conn, err := grpc.Dial(":21250",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(UnaryClientInterceptor(
			&DialConfig{ThrottlingDuration: 10 * time.Millisecond, MaxRetryConnect: 3, DefaultDNS: ":21250"},
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
	// correct server
	var header metadata.MD // variable to store header and trailer
	c, cancel := MakeContext(20, nil)
	defer cancel()
	c = metadata.AppendToOutgoingContext(c, "s_key", "thanh")
	resp, err := client.ListVisitors(c, &pb.VisitorRequest{AccountId: "thanh"}, grpc.Header(&header))
	// log.Print("header", header)
	log.Print(resp, err, header)
	if resp.Total == 0 {
		t.Fail()
	}
	log.Print("------------------------------------------------------------------------------------------------")
	// must redirect
	var header2 metadata.MD // variable to store header and trailer
	c1, cancel1 := MakeContext(20, nil)
	defer cancel1()
	c1 = metadata.AppendToOutgoingContext(c1, "s_key", "thanh1")
	resp, err = client.ListVisitors(c1, &pb.VisitorRequest{AccountId: "thanh1"}, grpc.Header(&header2))
	log.Print(resp, err, header2)

	// if strings.Join(header2.Get("shard_addrs"), "") != ":21250:21251" {
	// 	t.Fatal("SHOULD REDIRECT", strings.Join(header2.Get("shard_addrs"), ""))
	// }
	// if resp.Total == 0 {
	// 	t.Fail()
	// }
	log.Print("------------------------------------------------------------------------------------------------")
	// we have learn about the servers, should not redirect agan
	var header3 metadata.MD // variable to store header and trailer
	c2, cancel1 := MakeContext(20, nil)
	defer cancel1()
	c2 = metadata.AppendToOutgoingContext(c2, "s_key", "thanh")
	resp, err = client.ListVisitors(c2, &pb.VisitorRequest{AccountId: "thanh"}, grpc.Header(&header3))
	log.Print(resp, err, header3)
	// if strings.Join(header3.Get("total_shards"), "") != "" {
	// 	t.Fatal("SHOULD NOT REDIRECT")
	// }
	log.Print("------------------------------------------------------------------------------------------------")
	// correct server
	var header4 metadata.MD // variable to store header and trailer
	c3, cancel1 := MakeContext(20, nil)
	defer cancel1()
	c3 = metadata.AppendToOutgoingContext(c3, "s_key", "thanh1")
	resp, err = client.ListVisitors(c3, &pb.VisitorRequest{}, grpc.Header(&header4))
	log.Print(resp, err, header4)
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
		// grpc.WithUnaryInterceptor(UnaryClientInterceptorV2()),
	)
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
	serviceDomain := "a12.staging.cluster.local"
	port := "6000"
	hostname := "a12-getcode-1"
	arr := strings.Split(hostname, "-")
	// app, index
	// parts := strings.Split(hostname, "-")
	if len(arr) < 2 {
		log.Panicf("hostname '%s' not valid form xxx-i", hostname)
	}
	app := strings.Join(arr[:len(arr)-1], "-")
	// get -> i:PPPP

	// get i
	index, err := strconv.Atoi(arr[len(arr)-1])
	if err != nil {
		log.Panicf("hostname not include index, err: %s", err.Error())
	}
	serviceAddrs := []string{}
	for i := 0; i < 5; i++ {
		// if i == 1 {
		// 	serviceAddrs = append(serviceAddrs, arr[0]+"-"+strconv.Itoa(i)+":21241")
		// 	continue
		// }
		// if i == 0 {
		// 	serviceAddrs = append(serviceAddrs, arr[0]+"-"+strconv.Itoa(i)+":21240")
		// 	continue
		// }
		// like a12-getcode-0.a12.staging.svc.cluster.local:6000
		serviceAddrs = append(serviceAddrs, app+"-"+strconv.Itoa(i)+"."+serviceDomain+":"+port)
	}
	log.Print(serviceAddrs, index)
}
