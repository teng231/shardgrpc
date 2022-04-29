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
)

// UnaryServerInterceptorV2Statefullset is called on every request received from a client to a
// unary server operation, here, we pull out the client operating system from
// the metadata, and inspect the context to receive the IP address that the
// request was received from. We then modify the EdgeLocation type to include
// this information for every request
// in statefull pod-name hostname like: web-0, web-1, ... web-i
func UnaryServerInterceptorV2Statefullset(hostname string, totalShard int) grpc.UnaryServerInterceptor {
	if hostname == "" {
		hostname = os.Getenv("HOSTNAME")
	}
	arr := strings.Split(hostname, "-")
	if len(arr) != 2 {
		log.Panicf("hostname '%s' not valid form xxx-i", hostname)
	}
	index, err := strconv.Atoi(arr[1])
	if err != nil {
		log.Panicf("hostname not include index, err: %s", err.Error())
	}
	serviceAddrs := []string{}
	for i := 0; i < totalShard; i++ {
		serviceAddrs = append(serviceAddrs, arr[0]+"-"+strconv.FormatInt(int64(i), 10))
	}
	return UnaryServerInterceptorV2(serviceAddrs, index)
}

// UnaryServerInterceptorV2 is called on every request received from a client to a
// unary server operation, here, we pull out the client operating system from
// the metadata, and inspect the context to receive the IP address that the
// request was received from. We then modify the EdgeLocation type to include
// this information for every request
func UnaryServerInterceptorV2(serviceAddrs []string, id int) grpc.UnaryServerInterceptor {
	// holds the current maximum number of shards
	// shardCount := len(serviceAddrs)

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (out interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("not extracted: undefined error %v", r)
			}
		}()
		flog(" [server] run heree ", serviceAddrs[id])
		// Get the metadata from the incoming context
		md, ok := metadata.FromIncomingContext(ctx)
		// non-sharding normal process
		if len(md[shard_running]) == 0 {
			handler(ctx, req)
		}
		if !ok {
			return nil, fmt.Errorf("couldn't parse incoming context metadata")
		}
		flog(md)
		// want get from md `shard_key`
		skey := GetShardKey(ctx, req)
		// recheck `shard_key` extractlly
		// if calcId not equal with id. need forward to extract grpc server.
		extractAddr, sNum := ShardKeyCalc(skey, serviceAddrs)
		header := metadata.New(nil)
		header.Set(shard_addrs, serviceAddrs...)
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
