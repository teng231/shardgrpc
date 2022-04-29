package shardgrpc

import (
	"context"
	"hash/crc32"
	"reflect"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type MetadataInjective struct {
}

func ShardKeyCalc(skey string, addrs []string) (string, int) {
	index := int(crc32.ChecksumIEEE([]byte(skey))) % len(addrs)
	flog(skey, " ", index)
	host := addrs[index]
	return host, index
}

// UnaryClientInterceptor is called on every request from a client to a unary
// server operation, here, we grab the operating system of the client and add it
// to the metadata within the context of the request so that it can be received
// by the server
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	mConn := make(map[string]*grpc.ClientConn)
	lock := &sync.RWMutex{}

	addrs := make([]string, 0)
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		flog("addrs", addrs)
		if len(addrs) == 0 {
			var header metadata.MD
			opts = append(opts, grpc.Header(&header))
			// automatic call to grpc server
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err != nil {
				flog(err)
				return err
			}
			flog("header 2222 ", header)

			if len(header[shard_addrs]) > 0 {
				lock.RLock()
				addrs = header.Get(shard_addrs)
				lock.RUnlock()
			}
			if len(addrs) == 0 {
				flog("addrs still empty")
			}
			return nil
		}
		// get extract shardKey
		skey := GetShardKey(ctx, req)
		addr, _ := ShardKeyCalc(skey, addrs)
		co, has := mConn[addr]
		if !has {
			var err error
			co, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			flog("dial ", addr)
			lock.RLock()
			mConn[addr] = co
			lock.RUnlock()
		}
		var header metadata.MD // variable to store header and trailer
		opts = append([]grpc.CallOption{grpc.Header(&header)}, opts...)
		err := co.Invoke(ctx, method, req, reply, opts...)
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

func GetShardKey(ctx context.Context, message interface{}) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md[shard_key]) > 0 {
		return md[shard_key][0]
	}
	if message == nil {
		return ""
	}
	msgrefl := message.(proto.Message).ProtoReflect()
	accIdDesc := msgrefl.Descriptor().Fields().ByName(account_id)
	if accIdDesc == nil {
		return ""
	}

	return msgrefl.Get(accIdDesc).String()
}
