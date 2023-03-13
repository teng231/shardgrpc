package shardgrpc

import "testing"

func TestShardIndex(t *testing.T) {
	cases := map[string]int{
		"api-5":             5,
		"a12-getcode-3":     3,
		"a12-getcode-abc-4": 4,
		// "a19-getlink-XDS32-1s3r": 0,
	}
	for host, index := range cases {
		i := shardIndex(host)
		if i != index {
			t.Fail()
		}
	}
}
