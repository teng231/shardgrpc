package shardgrpc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type DialConfig struct {
	MaxRetryConnect    int
	ThrottlingDuration time.Duration
	DefaultDNS         string
}

const (
	TransportError = "transport: error while dialing"
)

func nonAddressCaller(dialConfig *DialConfig, dialOpts []grpc.DialOption, ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) ([]string, error) {
	var header metadata.MD
	opts = append(opts, grpc.Header(&header))
	// automatic call to grpc server
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), TransportError) {
		co, err := grpc.Dial(dialConfig.DefaultDNS, dialOpts...)
		if err != nil {
			return nil, fmt.Errorf("dial error: %s", err.Error())
		}
		if err := invoker(ctx, method, req, reply, co, opts...); err != nil {
			return nil, fmt.Errorf("invoker fail %s", err.Error())
		}
	}
	addrs := make([]string, 0)
	if len(header[shard_addrs]) > 0 {
		addrs = header[shard_addrs]
	}
	if len(addrs) == 0 {
		log.Print("addrs still empty")
		return nil, errors.New("no address to connect")
	}
	return addrs, nil
}

// UnaryClientInterceptor is called on every request from a client to a unary
// server operation, here, we grab the operating system of the client and add it
// to the metadata within the context of the request so that it can be received
// by the server
func UnaryClientInterceptor(dialConfig *DialConfig, dialOpts ...grpc.DialOption) grpc.UnaryClientInterceptor {
	mConn := make(map[string]*grpc.ClientConn)
	lock := &sync.RWMutex{}
	addrs := make([]string, 0)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// At first time: have not info of all address
		// Dial 1 of them to get all shard address then append to map
		if len(addrs) == 0 {
			lock.Lock()
			defer lock.Unlock()
			addrOut, err := nonAddressCaller(dialConfig, dialOpts, ctx, method, req, reply, cc, invoker, opts...)
			addrs = addrOut
			return err
		}

		// this case work for connection working, addrs has resolved
		// just action with server
		var header metadata.MD
		skey := GetClientShardKey(ctx, req)
		if len(addrs) == 0 {
			return errors.New("no address to connect")
		}
		addr, _ := calcAddress(skey, addrs)
		lock.RLock()
		co, has := mConn[addr]
		lock.RUnlock()
		if !has {
			newConn, err := grpc.Dial(addr, dialOpts...)
			if err != nil {
				return fmt.Errorf("connect addrs fail %s", err.Error())
			}
			lock.Lock()
			mConn[addr] = newConn
			co = newConn
			lock.Unlock()
		}
		// var header metadata.MD // variable to store header and trailer
		opts = append([]grpc.CallOption{grpc.Header(&header)}, opts...)
		if err := co.Invoke(ctx, method, req, reply, opts...); err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), TransportError) {
				return err
			}
		}
		return nil
	}
}
