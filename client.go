package shardgrpc

import (
	"context"
	"errors"
	"hash/crc32"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type DialConfig struct {
	MaxRetryConnect    int
	ThrottlingDuration time.Duration
	DefaultDNS         string
}

func GetShardAddressFromShardKey(skey string, addrs []string) (string, int) {
	index := int(crc32.ChecksumIEEE([]byte(skey))) % len(addrs)
	flog(skey, " ", index)
	host := addrs[index]
	return host, index
}

func tryDial(addr string, dialConfig *DialConfig, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	for i := 0; i < dialConfig.MaxRetryConnect; i++ {
		co, err := grpc.Dial(addr, opts...)
		if err == nil {
			return co, nil
		}
		time.Sleep(dialConfig.ThrottlingDuration)
	}
	return nil, errors.New("can't retry connection to: " + addr)
}

func tryInvoke(fnInvoke func(cc *grpc.ClientConn) error,
	lock *sync.RWMutex, mConn map[string]*grpc.ClientConn, addr string, dialConfig *DialConfig,
	opts ...grpc.DialOption) error {
	co, err := tryDial(addr, dialConfig, opts...)
	if err != nil {
		return err
	}
	lock.Lock()
	mConn[addr] = co
	lock.Unlock()
	log.Print("retry invoke")
	err = fnInvoke(co)
	return err
}

// UnaryClientInterceptor is called on every request from a client to a unary
// server operation, here, we grab the operating system of the client and add it
// to the metadata within the context of the request so that it can be received
// by the server
func UnaryClientInterceptor(dialConfig *DialConfig, dialOpts ...grpc.DialOption) grpc.UnaryClientInterceptor {
	mConn := make(map[string]*grpc.ClientConn)
	lock := &sync.RWMutex{}
	addrs := make([]string, 0)

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// At first time: have not info of all address
		// Dial 1 of them to get all shard address then append to map
		if len(addrs) == 0 {
			var header metadata.MD
			opts = append(opts, grpc.Header(&header))
			// automatic call to grpc server
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err != nil {
				err = tryInvoke(func(cc *grpc.ClientConn) error {
					err = cc.Invoke(ctx, method, req, reply, opts...)
					return err
				}, lock, mConn, dialConfig.DefaultDNS, dialConfig, dialOpts...)
				log.Print(err)
			}
			// if header have shard_redirect value is need change process
			if val := header.Get(shard_redirected); strings.Join(val, "") != "" {
				addr := strings.Join(val, "")
				lock.RLock()
				co, has := mConn[addr]
				lock.RUnlock()
				if !has {
					// _co, err := tryDial(addr, dialConfig, dialOpts...)
					// if err != nil {
					// 	return err
					// }
					// lock.Lock()
					// mConn[addr] = _co
					// lock.Unlock()
					// co = _co
					tryInvoke(func(cc *grpc.ClientConn) error {
						err = cc.Invoke(ctx, method, req, reply, opts...)
						return err
					}, lock, mConn, addr, dialConfig, dialOpts...)
				} else {
					err = co.Invoke(ctx, method, req, reply, opts...)
				}

			}
			if err != nil {
				log.Print("[client] ", err)
				return err
			}
			if len(header[shard_addrs]) > 0 {
				addrs = header[shard_addrs]
			}
			if len(addrs) == 0 {
				log.Print("[client] addrs still empty")
			}
			return nil
		}
		// this case work for connection working, addrs has resolved
		// just action with server
		var err error
		var header metadata.MD
		skey := GetClientShardKey(ctx, req)
		if len(addrs) == 0 {
			panic("not found addrs")
		}
		addr, _ := GetShardAddressFromShardKey(skey, addrs)
		lock.RLock()
		co, has := mConn[addr]
		lock.RUnlock()
		if !has {
			co, err = tryDial(addr, dialConfig, dialOpts...)
			if err != nil {
				return err
			}
			lock.Lock()
			mConn[addr] = co
			lock.Unlock()
		}
		// var header metadata.MD // variable to store header and trailer
		opts = append([]grpc.CallOption{grpc.Header(&header)}, opts...)
		err = co.Invoke(ctx, method, req, reply, opts...)
		if err != nil {
			log.Print("+++ Connection break: maybe some ip changed +++")
			// need retry now
			err = tryInvoke(func(cc *grpc.ClientConn) error {
				err = cc.Invoke(ctx, method, req, reply, opts...)
				return err
			}, lock, mConn, addr, dialConfig, dialOpts...)
		}
		return err
	}
}

// getReturnType returns the return types for a GRPC method
// the method name should be full method name (i.e., /package.service/method)
// For example, with handler
//   (s *server) func Goodbye() string {}
//   (s *server) func Ping(_ context.Context, _ *pb.Ping) (*pb.Pong, error) {}
//   (s *server) func Hello(_ context.Context, _ *pb.Empty) (*pb.String, error) {}
func getReturnType(server interface{}, fullmethod string) reflect.Type {
	flog(server, "  ", fullmethod)
	t := reflect.TypeOf(server)
	for i := 0; i < t.NumMethod(); i++ {
		methodType := t.Method(i).Type

		if !strings.HasSuffix(fullmethod, "/"+t.Method(i).Name) {
			continue
		}

		if methodType.NumOut() != 2 || methodType.NumIn() < 2 {
			continue
		}

		// the first parameter should context and the second one should be a pointer
		if methodType.In(1).Name() != "Context" || methodType.In(2).Kind() != reflect.Ptr {
			continue
		}

		// the first output should be a pointer and the second one should be an error
		if methodType.Out(0).Kind() != reflect.Ptr || methodType.Out(1).Name() != "error" {
			continue
		}

		return methodType.Out(0).Elem()

	}
	return nil
}

func GetClientShardKey(ctx context.Context, message interface{}) string {
	md, _ := metadata.FromOutgoingContext(ctx)
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
