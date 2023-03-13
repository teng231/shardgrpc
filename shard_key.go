package shardgrpc

import (
	"context"
	"hash/crc32"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func serverShardKey(ctx context.Context, message interface{}) string {
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
	flog(skey, " ", index)
	host := addrs[index]
	return host, index
}
