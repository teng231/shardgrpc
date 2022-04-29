/*
	define all common type or definations of
*/

package shardgrpc

import (
	"encoding/json"
	"log"
	"os"
)

const (
	shard_key           = "shard_key"
	shard_addrs         = "shard_addrs"
	shard_redirected    = "shard_redirected"
	shard_forwarded_for = "shard_forwarded_for"
	account_id          = "account_id"
	shard_running       = "true"
)

var (
	enableLog = os.Getenv("SHARD_ENABLE_LOG") == "true"
)

func jsonLog(i interface{}) {
	bin, _ := json.MarshalIndent(i, " ", " ")
	log.Print(string(bin))
}

func flog(data ...interface{}) {
	if !enableLog {
		return
	}
	log.Print(data)
}
