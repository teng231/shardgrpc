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
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
)

const (
	MAX_BACKOFF = 3
)

type IClient interface {
	UnaryClientInterceptor(dialConfig *DialConfig, dialOpts ...grpc.DialOption) grpc.UnaryClientInterceptor
	GetShardCount() int
	GetAddrs() []string
	CalcShardIndexByKey(key string) int
}

type Client struct {
	// shardCount int
	addrs []string
	mConn map[string]*grpc.ClientConn
	lock  *sync.RWMutex
}

func CreateShardClient() *Client {
	return &Client{
		addrs: make([]string, 0),
		mConn: make(map[string]*grpc.ClientConn),
		lock:  &sync.RWMutex{},
	}
}

func (s *Client) GetShardCount() int {
	return len(s.addrs)
}

func (s *Client) GetAddrs() []string {
	return s.addrs
}

func (s *Client) CalcShardIndexByKey(key string) int {
	_, index := calcAddress(key, s.addrs)
	return index
}

func (s *Client) nonAddressCaller(dialConfig *DialConfig, dialOpts []grpc.DialOption, ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	var header metadata.MD
	opts = append(opts, grpc.Header(&header))
	// automatic call to grpc server
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), TransportError) {
		co, err := grpc.Dial(dialConfig.DefaultDNS, dialOpts...)
		if err != nil {
			return fmt.Errorf("dial error: %s", err.Error())
		}
		if err := invoker(ctx, method, req, reply, co, opts...); err != nil {
			return fmt.Errorf("invoker fail %s", err.Error())
		}
	}
	if len(header[shard_addrs]) > 0 {
		s.addrs = header[shard_addrs]
	}
	if len(s.addrs) == 0 {
		log.Print("addrs still empty")
		return errors.New("no address to connect")
	}
	return nil
}

// UnaryClientInterceptor is called on every request from a client to a unary
// server operation, here, we grab the operating system of the client and add it
// to the metadata within the context of the request so that it can be received
// by the server
func (s *Client) UnaryClientInterceptor(dialConfig *DialConfig, dialOpts ...grpc.DialOption) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// At first time: have not info of all address
		// Dial 1 of them to get all shard address then append to map
		if len(s.addrs) == 0 {
			s.lock.Lock()
			defer s.lock.Unlock()
			return s.nonAddressCaller(dialConfig, dialOpts, ctx, method, req, reply, cc, invoker, opts...)
		}
		// check connection is online

		// this case work for connection working, addrs has resolved
		// just action with server
		var header metadata.MD
		skey := GetClientShardKey(ctx, req)
		if len(s.addrs) == 0 {
			return errors.New("no address to connect")
		}
		addr, _ := calcAddress(skey, s.addrs)
		var connErr error
		var co *grpc.ClientConn
		var has bool
		for i := 0; i < MAX_BACKOFF; i++ {
			connErr = nil
			s.lock.RLock()
			co, has = s.mConn[addr]
			s.lock.RUnlock()

			if !has {
				newConn, err := grpc.Dial(addr, dialOpts...)
				if err != nil {
					connErr = err
					// return fmt.Errorf("connect addrs fail %s", err.Error())
					continue
				}
				s.lock.Lock()
				s.mConn[addr] = newConn
				co = newConn
				s.lock.Unlock()
				break
			}
			log.Print(co.GetState())
			if co.GetState() == connectivity.TransientFailure {
				// connection with server temporary closed
				s.lock.Lock()
				// remove old connection.
				// delete(s.mConn, addr)
				s.mConn = make(map[string]*grpc.ClientConn)
				s.lock.Unlock()
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
		if connErr != nil {
			return fmt.Errorf("connect addrs fail %s", connErr.Error())
		}

		// var header metadata.MD // variable to store header and trailer
		opts = append([]grpc.CallOption{grpc.Header(&header)}, opts...)
		if err := co.Invoke(ctx, method, req, reply, opts...); err != nil {
			log.Print(err)
			if !strings.Contains(strings.ToLower(err.Error()), TransportError) {
				return fmt.Errorf("invoke error: %s", err)
			}
		}
		return nil
	}
}
