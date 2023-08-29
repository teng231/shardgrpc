package shardgrpc

import (
	"context"
	"hash/crc32"
	"reflect"
	"strings"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func hashingKey(ctx context.Context, message interface{}) string {
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

func calcAddress(skey string, addrs []string) (string, int) {
	index := int(crc32.ChecksumIEEE([]byte(skey))) % len(addrs)
	host := addrs[index]
	return host, index
}

// getReturnType returns the return types for a GRPC method
// the method name should be full method name (i.e., /package.service/method)
// For example, with handler
//
//	(s *server) func Goodbye() string {}
//	(s *server) func Ping(_ context.Context, _ *pb.Ping) (*pb.Pong, error) {}
//	(s *server) func Hello(_ context.Context, _ *pb.Empty) (*pb.String, error) {}
func getReturnType(server any, fullmethod string) reflect.Type {
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
