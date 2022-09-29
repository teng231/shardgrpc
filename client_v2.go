package shardgrpc

import (
	"context"
	"log"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// func appendToOutgoingContext(ctx context.Context, key, value string) context.Context {
// 	ctx = metadata.AppendToOutgoingContext(ctx, key, string(value))
// 	return ctx
// }

func clientCustomInvoke(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, addrs []string, lock *sync.RWMutex, mConn map[string]*grpc.ClientConn, opts ...grpc.CallOption) (metadata.MD, error) {
	// calculate shard key to find extract server
	// get shard address from this server

	skey := GetClientShardKey(ctx, req)

	if len(addrs) == 0 {
		panic("not found addrs")
	}

	addr, _ := GetShardAddressFromShardKey(skey, addrs)
	lock.RLock()
	co, has := mConn[addr]
	lock.RUnlock()
	if !has {
		var err error
		co, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		flog(" [client] dial ", addr)
		lock.RLock()
		mConn[addr] = co
		lock.RUnlock()
	}
	var header metadata.MD // variable to store header and trailer
	opts = append([]grpc.CallOption{grpc.Header(&header)}, opts...)
	err := co.Invoke(ctx, method, req, reply, opts...)
	return header, err
}

// UnaryClientInterceptorV2 is called on every request from a client to a unary
// server operation, here, we grab the operating system of the client and add it
// to the metadata within the context of the request so that it can be received
// by the server
func UnaryClientInterceptorV2() grpc.UnaryClientInterceptor {
	mConn := make(map[string]*grpc.ClientConn)
	lock := &sync.RWMutex{}
	addrs := make([]string, 0)
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// At first time: have not info of all address
		// Dial 1 of them to get all shard address then append to map
		// log.Print(11111)
		if len(addrs) == 0 {
			var header metadata.MD
			opts = append(opts, grpc.Header(&header))
			// automatic call to grpc server
			err := invoker(ctx, method, req, reply, cc, opts...)
			// if header have shard_redirect value is need change process
			if val := header.Get(shard_redirected); strings.Join(val, "") != "" {
				addr := strings.Join(val, "")
				co, has := mConn[addr]
				if !has {
					var err error
					co, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						return err
					}
					lock.Lock()
					mConn[addr] = co
					lock.Unlock()
				}
				err = invoker(ctx, method, req, reply, co, opts...)
			}
			if err != nil {
				log.Print("[client] ", err)
				return err
			}
			if len(header[shard_addrs]) > 0 {
				lock.RLock()
				addrs = header[shard_addrs]
				lock.RUnlock()
			}
			if len(addrs) == 0 {
				log.Print("[client] addrs still empty")
			}
			return nil
		}
		MAXREIES := 5
		var err error
		var header metadata.MD
		for i := 0; i < MAXREIES; i++ {
			header, err = clientCustomInvoke(ctx, method, req, reply, cc, invoker, addrs, lock, mConn, opts...)
			flog(" [client] client out", header, err)
			if err == nil {
				if val := strings.Join(header[shard_redirected], ""); val == "" {
					return err
				}
				// return nil
			}
			// header trigger
			if len(header[shard_addrs]) > 0 {
				lock.RLock()
				addrs = header[shard_addrs]
				lock.RUnlock()
			}
		}
		return err
	}
}
