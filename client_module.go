package shardgrpc

import (
	"context"
	"log"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type IClient interface {
	UnaryClientInterceptor(dialConfig *DialConfig, dialOpts ...grpc.DialOption) grpc.UnaryClientInterceptor
	GetShardCount() int
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

// UnaryClientInterceptor is called on every request from a client to a unary
// server operation, here, we grab the operating system of the client and add it
// to the metadata within the context of the request so that it can be received
// by the server
func (s *Client) UnaryClientInterceptor(dialConfig *DialConfig, dialOpts ...grpc.DialOption) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// At first time: have not info of all address
		// Dial 1 of them to get all shard address then append to map
		if len(s.addrs) == 0 {
			var header metadata.MD
			opts = append(opts, grpc.Header(&header))
			// automatic call to grpc server
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err != nil {
				err = tryInvoke(func(cc *grpc.ClientConn) error {
					err = cc.Invoke(ctx, method, req, reply, opts...)
					return err
				}, s.lock, s.mConn, dialConfig.DefaultDNS, dialConfig, dialOpts...)
				log.Print(err)
			}
			// if header have shard_redirect value is need change process
			if val := header.Get(shard_redirected); strings.Join(val, "") != "" {
				addr := strings.Join(val, "")
				s.lock.RLock()
				co, has := s.mConn[addr]
				s.lock.RUnlock()
				if !has {
					tryInvoke(func(cc *grpc.ClientConn) error {
						err = cc.Invoke(ctx, method, req, reply, opts...)
						return err
					}, s.lock, s.mConn, addr, dialConfig, dialOpts...)
				} else {
					err = co.Invoke(ctx, method, req, reply, opts...)
				}

			}
			if err != nil {
				log.Print("[client] ", err)
				return err
			}
			if len(header[shard_addrs]) > 0 {
				s.addrs = header[shard_addrs]
			}
			if len(s.addrs) == 0 {
				log.Print("[client] addrs still empty")
			}
			return nil
		}
		// this case work for connection working, addrs has resolved
		// just action with server
		var err error
		var header metadata.MD
		skey := GetClientShardKey(ctx, req)
		if len(s.addrs) == 0 {
			panic("not found addrs")
		}
		addr, _ := GetShardAddressFromShardKey(skey, s.addrs)
		s.lock.RLock()
		co, has := s.mConn[addr]
		s.lock.RUnlock()
		if !has {
			co, err = tryDial(addr, dialConfig, dialOpts...)
			if err != nil {
				return err
			}
			s.lock.Lock()
			s.mConn[addr] = co
			s.lock.Unlock()
		}
		// var header metadata.MD // variable to store header and trailer
		opts = append([]grpc.CallOption{grpc.Header(&header)}, opts...)
		err = co.Invoke(ctx, method, req, reply, opts...)
		if err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), TransportError) {
				return err
			}
			log.Print("+++ Connection break: maybe some ip changed +++")
			// need retry now
			err = tryInvoke(func(cc *grpc.ClientConn) error {
				err = cc.Invoke(ctx, method, req, reply, opts...)
				return err
			}, s.lock, s.mConn, addr, dialConfig, dialOpts...)
		}
		return err
	}
}
