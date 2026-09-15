package config

import (
	"testing"
	"time"
)

func TestFromEnvShutdownTimeout(t *testing.T) {
	t.Setenv("STATEFLUX_SERVER_SHUTDOWN_TIMEOUT", "3s")
	cfg := FromEnv()
	if cfg.Server.ShutdownTimeout != 3*time.Second {
		t.Fatalf("ShutdownTimeout = %s, want 3s", cfg.Server.ShutdownTimeout)
	}
}

func TestDefaultShutdownTimeout(t *testing.T) {
	cfg := Default()
	if cfg.Server.ShutdownTimeout <= 0 {
		t.Fatalf("default ShutdownTimeout = %s, want positive", cfg.Server.ShutdownTimeout)
	}
}
