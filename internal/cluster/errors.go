package cluster

import "fmt"

func errUnknownTransport(t string) error {
	return fmt.Errorf("cluster: unknown transport %q (want grpc|http|static)", t)
}
