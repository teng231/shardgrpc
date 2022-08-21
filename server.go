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
)

// UnaryServerInterceptorStatefullset is called on every request received from a client to a
// unary server operation, here, we pull out the client operating system from
// the metadata, and inspect the context to receive the IP address that the
// request was received from. We then modify the EdgeLocation type to include
// this information for every request
// in statefull pod-name hostname like: web-0, web-1, ... web-i
func UnaryServerInterceptorStatefullset(hostname string, totalShard int) grpc.UnaryServerInterceptor {
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
	return UnaryServerInterceptor(serviceAddrs, index)
}

// UnaryServerInterceptor is called on every request received from a client to a
// unary server operation, here, we pull out the client operating system from
// the metadata, and inspect the context to receive the IP address that the
// request was received from. We then modify the EdgeLocation type to include
// this information for every request
func UnaryServerInterceptor(serviceAddrs []string, id int) grpc.UnaryServerInterceptor {
	// holds the current maximum number of shards
	// shardCount := len(serviceAddrs)
	lock := &sync.RWMutex{}
	conn := make(map[string]*grpc.ClientConn)

	// in order to proxy (forward) the request to another grpc host,
	// we must have an output object of the request's method (so we can marshal the response).
	// we are going to build a map of returning type for all methods of the server. And do it
	// only once time for each method name right before the first request.
	// mReturntype := make(map[string]reflect.Type)
	// lockReturnType := &sync.RWMutex{}
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (out interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("not extracted: undefined error %v", r)
			}
		}()
		flog("run heree")
		// Get the metadata from the incoming context
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, fmt.Errorf("couldn't parse incoming context metadata")
		}
		flog("[service] ", md)
		// want get from md `shard_key`
		skey := GetServerShardKey(ctx, req)
		// recheck `shard_key` extractlly
		// if calcId not equal with id. need forward to extract grpc server.
		extractAddr, sNum := GetShardAddressFromShardKey(skey, serviceAddrs)
		header := metadata.New(nil)
		header.Set(shard_addrs, serviceAddrs...)
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
		cc, ok := conn[extractAddr]
		if !ok {
			cc, err = grpc.Dial(extractAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			lock.RLock()
			conn[extractAddr] = cc
			lock.RUnlock()
		}
		// making a map of returning type for all methods of the server
		// returntype := mReturntype[info.FullMethod]
		// if returntype == nil {
		// 	returntype =
		// 		lockReturnType.RLock()
		// 	mReturntype[info.FullMethod] = returntype
		// 	lockReturnType.RUnlock()
		// }
		out = reflect.New(getReturnType(info.Server, info.FullMethod)).Interface()

		extraHeader := metadata.New(nil)
		extraHeader.Set(shard_forwarded_for, serviceAddrs[id])
		err = forward(ctx, info.FullMethod, req, out, cc, extraHeader)
		return out, err
	}
}

// forward proxy a GRPC calls to another host, header and trailer are preserved
// parameters:
//   host: host address which will be redirected to
//   method: the full RPC method string, i.e., /package.service/method.
//   in: value of input (in request) parameter
// this method returns output just like a normal GRPC call
func forward(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, extrasHeader metadata.MD) error {
	md, _ := metadata.FromIncomingContext(ctx)
	extrasHeader = metadata.Join(extrasHeader, md)
	outctx := metadata.NewOutgoingContext(context.Background(), extrasHeader)

	var header metadata.MD
	err := cc.Invoke(outctx, method, req, reply, grpc.Header(&header))
	if err != nil {
		return err
	}
	grpc.SendHeader(ctx, header)
	flog("[server] ", err, reply)
	return err
}
