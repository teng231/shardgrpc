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
	shard_key           = "s_key"
	shard_addrs         = "s_addrs"
	shard_redirected    = "s_redirected"
	shard_forwarded_for = "s_forwarded_for"
	account_id          = "account_id"
	shard_running       = "s_running"
)

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
