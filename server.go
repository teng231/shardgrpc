package shardgrpc

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
)

const (
	shard_key         = "s_key"   // shard key data
	shard_addrs       = "s_addrs" // list address of grpc
	shard_redirected  = "s_redirected"
	shard_default_key = "s_default_key" // field default shardkey
	shard_running     = "s_running"     // name shard run this request
)

func init() {
	resolver.SetDefaultScheme("dns")
	log.SetFlags(log.Lshortfile)
}

type ServerInfo struct {
	Host string
	Port int
}

func GetShardIndex(hostname string) int {
	if hostname == "" {
		hostname = os.Getenv("HOSTNAME")
	}
	arr := strings.Split(hostname, "-")
	if len(arr) < 2 {
		log.Panicf("hostname '%s' not valid form xxx-i", hostname)
	}
	index, err := strconv.Atoi(arr[len(arr)-1])
	if err != nil {
		log.Panicf("hostname not include index, err: %s", err.Error())
	}
	return index
}

// UnaryServerInterceptorPreset is called on every request received from a client to a
// unary server operation, here, we pull out the client operating system from
// the metadata, and inspect the context to receive the IP address that the
// request was received from. We then modify the EdgeLocation type to include
// this information for every request
// normal case servers like [{host: localhost, port: 8001},{host: localhost, port: 8002}]
func UnaryServerInterceptorPreset(servers []*ServerInfo, currentAddr string) grpc.UnaryServerInterceptor {
	serviceAddrs := []string{}
	index := 0
	for idx, ser := range servers {
		addr := ser.Host + ":" + strconv.Itoa(ser.Port)
		serviceAddrs = append(serviceAddrs, addr)
		if addr == currentAddr {
			index = idx
		}
	}
	log.Print(serviceAddrs)
	return UnaryServerInterceptor(serviceAddrs, index)
}

// UnaryServerInterceptorStatefullset is called on every request received from a client to a
// unary server operation, here, we pull out the client operating system from
// the metadata, and inspect the context to receive the IP address that the
// request was received from. We then modify the EdgeLocation type to include
// this information for every request
// in statefull pod-name hostname like: web-0:3456, web-1:3456, ... web-i:3456
// serviceDomain like a12.staging.svc.cluster.local
func UnaryServerInterceptorStatefullset(hostname, port, serviceDomain string, totalShard int) grpc.UnaryServerInterceptor {
	if hostname == "" {
		hostname = os.Getenv("HOSTNAME")
	}
	arr := strings.Split(hostname, "-")
	if len(arr) < 2 {
		log.Panicf("hostname '%s' not valid form xxx-i", hostname)
	}
	app := strings.Join(arr[:len(arr)-1], "-")
	index, err := strconv.Atoi(arr[len(arr)-1])
	if err != nil {
		log.Panicf("hostname not include index, err: %s", err.Error())
	}
	serviceAddrs := []string{}
	for i := 0; i < totalShard; i++ {
		serviceAddrs = append(serviceAddrs, app+"-"+strconv.Itoa(i)+"."+serviceDomain+":"+port)
	}
	log.Print(serviceAddrs)
	return UnaryServerInterceptor(serviceAddrs, index)
}

// forward proxy a GRPC calls to another host, header and trailer are preserved
// parameters:
//
//	host: host address which will be redirected to
//	method: the full RPC method string, i.e., /package.service/method.
//	returnedType: type of returned value
//	in: value of input (in request) parameter
//
// this method returns output just like a normal GRPC call
func forward(cc *grpc.ClientConn, fmethod string, ctx context.Context, in, server any, extraHeader metadata.MD) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	extraHeader = metadata.Join(extraHeader, md)
	outctx := metadata.NewOutgoingContext(context.Background(), extraHeader)

	out := reflect.New(getReturnType(server, fmethod)).Interface()
	var header metadata.MD
	err := cc.Invoke(outctx, fmethod, in, out, grpc.Header(&header))
	grpc.SendHeader(ctx, header)
	return out, err
}

// UnaryServerInterceptor is called on every request received from a client to a
// unary server operation, here, we pull out the client operating system from
// the metadata, and inspect the context to receive the IP address that the
// request was received from. We then modify the EdgeLocation type to include
// this information for every request
func UnaryServerInterceptor(addrs []string, id int) grpc.UnaryServerInterceptor {
	// holds the current maximum number of shards
	// shardCount := len(serviceAddrs)
	if len(addrs) == 0 {
		panic("not found addrs")
	}

	mConn := make(map[string]*grpc.ClientConn)
	mt := &sync.RWMutex{}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (out any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("not extracted: undefined error %v", r)
			}
		}()
		header, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			header = metadata.New(nil)
		}
		// want get from md `shard_key`
		skey := hashingKey(ctx, req)
		extractAddr, sNum := calcAddress(skey, addrs)
		header.Set(shard_addrs, addrs...)
		header.Set(shard_running, addrs[id])
		// if extract shard id with be processed
		if sNum == id {
			grpc.SendHeader(ctx, header)
			out, err := handler(ctx, req)
			return out, err
		}
		log.Printf("[%d] inconsistent%v ", id, extractAddr)
		header.Set(shard_redirected, extractAddr)

		// if wrong address, we need connect extract server address
		conn := mConn[extractAddr]
		if conn == nil {
			// step1: connect
			cc, err := grpc.Dial(extractAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			log.Printf("[%d] dial done to %s", id, extractAddr)
			mt.Lock()
			mConn[extractAddr] = cc
			conn = cc
			mt.Unlock()
		}
		// now time to proxy to extract addr
		// header now inject trustly shard_addrs
		// header add tracing that's this is now a proxy for other service.
		return forward(conn, info.FullMethod, ctx, req, info.Server, header)
	}
}
