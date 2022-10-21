/*
	define all common type or definations of
*/

package shardgrpc

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
)

const (
	shard_key           = "s_key"   // shard key data
	shard_addrs         = "s_addrs" // list address of grpc
	shard_redirected    = "s_redirected"
	shard_forwarded_for = "s_forwarded_for" // shard forward a -> b
	shard_default_key   = "s_default_key"   // field default shardkey
	shard_running       = "s_running"       // name shard run this request
)

func init() {
	resolver.SetDefaultScheme("dns")
	log.SetFlags(log.Lshortfile)
}

var (
	enableLog = os.Getenv("SHARD_ENABLE_LOG") == "true"
)

func jlog(i interface{}) {
	bin, _ := json.MarshalIndent(i, " ", " ")
	log.Print(string(bin))
}

func flog(data ...interface{}) {
	if !enableLog {
		return
	}
	log.Print(data...)
}

func CreateKeepAlive() grpc.DialOption {
	return grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                10 * time.Second,
		Timeout:             20 * time.Second,
		PermitWithoutStream: true,
	})
}
