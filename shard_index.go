package shardgrpc

import (
	"log"
	"os"
	"strconv"
	"strings"
)

func shardIndex(hostname string) int {
	if hostname == "" {
		hostname = os.Getenv("HOSTNAME")
	}
	arr := strings.Split(hostname, "-")
	if len(arr) < 2 {
		log.Panicf("hostname '%s' not valid form xxx-i", hostname)
	}
	index, err := strconv.Atoi(arr[len(arr)-1])
	if err != nil {
		log.Panicf("hostname not include index, err: %s", err.Error())
	}
	return index
}
