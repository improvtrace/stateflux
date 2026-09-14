package rpc

import "strconv"

func parseKey(key string) int64 {
	if key == "" {
		return 0
	}
	n, err := strconv.ParseInt(key, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
