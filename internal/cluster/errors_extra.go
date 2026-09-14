package cluster

import "fmt"

func errNoEndpoint(transport string) error {
	return fmt.Errorf("cluster: transport %s requires config.Cluster.Endpoint", transport)
}
