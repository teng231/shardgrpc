package shardgrpc

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func GetServerShardKey(ctx context.Context, message interface{}) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md[shard_key]) > 0 {
		return md[shard_key][0]
	}
	if message == nil {
		return ""
	}
	msgrefl := message.(proto.Message).ProtoReflect()
	defShardKey := msgrefl.Descriptor().Fields().ByName(shard_default_key)
	if defShardKey == nil {
		return ""
	}

	return msgrefl.Get(defShardKey).String()
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
		// if i == 1 {
		// 	serviceAddrs = append(serviceAddrs, app+"-"+strconv.Itoa(i)+":21241")
		// 	continue
		// }
		// if i == 0 {
		// 	serviceAddrs = append(serviceAddrs, app+"-"+strconv.Itoa(i)+":21240")
		// 	continue
		// }
		// like a12-getcode-0.a12.staging.svc.cluster.local:6000
		serviceAddrs = append(serviceAddrs, app+"-"+strconv.Itoa(i)+"."+serviceDomain+":"+port)
	}
	return UnaryServerInterceptor(serviceAddrs, index)
}

// UnaryServerInterceptorV2 is called on every request received from a client to a
// unary server operation, here, we pull out the client operating system from
// the metadata, and inspect the context to receive the IP address that the
// request was received from. We then modify the EdgeLocation type to include
// this information for every request
func UnaryServerInterceptor(serviceAddrs []string, id int) grpc.UnaryServerInterceptor {
	// holds the current maximum number of shards
	// shardCount := len(serviceAddrs)
	if len(serviceAddrs) == 0 {
		panic("not found serviceAddrs")
	}
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (out interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("not extracted: undefined error %v", r)
			}
		}()
		flog(" [server] run heree ", serviceAddrs[id])
		// Get the metadata from the incoming context
		header, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			// return nil, fmt.Errorf("couldn't parse incoming context metadata")
			header = metadata.New(nil)
		}
		flog("[server] ", header)
		// want get from md `shard_key`
		skey := GetServerShardKey(ctx, req)
		// recheck `shard_key` extractlly
		// if calcId not equal with id. need forward to extract grpc server.
		extractAddr, sNum := GetShardAddressFromShardKey(skey, serviceAddrs)
		flog(serviceAddrs[id], " ", skey, " ", serviceAddrs)
		header.Set(shard_addrs, serviceAddrs...)
		header.Set(shard_running, serviceAddrs[id])
		// if extract shard id with be processed
		if sNum == id {
			grpc.SendHeader(ctx, header)
			out, err := handler(ctx, req)
			return out, err
		}

		// now time to proxy to extract addr
		// header now inject trustly shard_addrs
		// header add tracing that's this is now a proxy for other service.
		header.Set(shard_redirected, extractAddr)
		grpc.SendHeader(ctx, header)
		out = reflect.New(getReturnType(info.Server, info.FullMethod)).Interface()
		return out, err
	}
}
