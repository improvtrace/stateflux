package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	dmpb "github.com/improvtrace/stateflux/api/dm"
	"github.com/improvtrace/stateflux/config"
)

const (
	// testKey 是测试用 dm 桩的服务密钥。
	testKey = "test-service-key"
	// waitTimeout 是等待 dm 桩就绪与退出的时限。
	waitTimeout = 5 * time.Second
)

// sockPath 返回临时目录下的 unix socket 路径。
func sockPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "dm.sock")
}

// waitReachable 轮询直到 socket 可拨通（dm 桩已监听），超时 Fatal。
func waitReachable(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("unix", address)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dm 桩未在 %v 内监听 %s", waitTimeout, address)
}

// startMock 以命令入口同一个 run 调起 dm 桩，等到可拨通后返回停止函数（取消
// ctx 并等 run 退出，返回其错误）；测试结束时自动停止。
func startMock(t *testing.T, address string, nodes int, nodeID string) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, config.DM{Address: address, ServiceKey: testKey}, nodes, nodeID, io.Discard) }()
	waitReachable(t, address)
	stop := sync.OnceValue(func() error {
		cancel()
		select {
		case err := <-errCh:
			return err
		case <-time.After(waitTimeout):
			return errors.New("dm 桩未随 ctx 取消退出")
		}
	})
	t.Cleanup(func() { assert.NoError(t, stop()) })
	return stop
}

// dialClient 按 dispatch 客户端的方式（unix socket + 服务密钥拦截器）连接 dm
// 桩；key 为空时不附加 service_key。
func dialClient(t *testing.T, address, key string) dmpb.ServiceServiceClient {
	t.Helper()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", address)
		}),
	}
	if key != "" {
		opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, fieldServiceKey, key)
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}))
	}
	conn, err := grpc.NewClient("passthrough:///"+address, opts...)
	if err != nil {
		t.Fatalf("连接 dm 桩失败: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return dmpb.NewServiceServiceClient(conn)
}

// TestNodeTable 断言按节点数生成的节点表：ID 依次为 node-N，首节点 LEADER、
// 其余 FOLLOWER，全部在线；响应携带的 node_id 即本节点 ID。
func TestNodeTable(t *testing.T) {
	for _, roles := range [][]string{
		{"LEADER"},
		{"LEADER", "FOLLOWER", "FOLLOWER"},
	} {
		table, err := nodeTable(len(roles), "node-1")
		if !assert.NoError(t, err) {
			continue
		}
		assert.Equal(t, "node-1", table.NodeId)
		if !assert.Len(t, table.Nodes, len(roles)) {
			continue
		}
		for i, n := range table.Nodes {
			id := fmt.Sprintf("node-%d", i+1)
			assert.Equal(t, id, n.Id)
			assert.Equal(t, id, n.Name)
			assert.Equal(t, fmt.Sprintf("127.0.0.%d", i+1), n.Ip)
			assert.Equal(t, n.Ip, n.BusinessIp)
			assert.Equal(t, roles[i], n.Role)
			assert.True(t, n.Online)
		}
	}

	// 本节点 ID 不在生成的表内即报错：该实例不会被路由到任何任务
	_, err := nodeTable(3, "node-4")
	assert.ErrorContains(t, err, "not in the generated table")

	table, err := nodeTable(3, "node-2")
	assert.NoError(t, err)
	assert.Equal(t, "node-2", table.NodeId)
}

// TestPrintNodeTable 断言启动时打印的节点表：含表头与全部节点的字段，且只有
// 本节点所在行带 (local) 标记。
func TestPrintNodeTable(t *testing.T) {
	table, err := nodeTable(3, "node-2")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	printNodeTable(&out, table)
	got := out.String()

	for _, want := range []string{
		"mock-dm: node table: 3 node(s)",
		"ID", "NAME", "ROLE", "ONLINE", "IP", "BUSINESS_IP",
		"node-1", "LEADER", "127.0.0.1",
		"node-2  (local)", "FOLLOWER", "127.0.0.2",
		"node-3", "127.0.0.3",
	} {
		assert.Contains(t, got, want)
	}
	assert.Equal(t, 1, strings.Count(got, "(local)"))
}

// TestNodeIP 断言序号到回环地址的推导：越过末位 255 后进位而不是失效。
func TestNodeIP(t *testing.T) {
	assert.Equal(t, "127.0.0.1", nodeIP(1))
	assert.Equal(t, "127.0.1.0", nodeIP(256))
	assert.Equal(t, "127.1.0.0", nodeIP(65536))
}

// TestRun 断言 dm 桩经 unix socket 提供生成的节点表，且服务密钥缺失或不符时
// 拒绝；每次调用都记录方法、调用方、结果码与耗时。
func TestRun(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	address := sockPath(t)
	startMock(t, address, 3, "node-2")
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	resp, err := dialClient(t, address, testKey).ListClusterNodesStatus(ctx, &emptypb.Empty{})
	if assert.NoError(t, err) {
		want, err := nodeTable(3, "node-2")
		assert.NoError(t, err)
		assert.Equal(t, want.String(), resp.String())
	}
	for name, key := range map[string]string{"服务密钥不符": "wrong-key", "缺少服务密钥": ""} {
		_, err := dialClient(t, address, key).ListClusterNodesStatus(ctx, &emptypb.Empty{})
		assert.Equal(t, codes.Unauthenticated, status.Code(err), name)
	}

	// 成功与拒绝的调用都留下日志，拒绝的调用带结果码与状态消息。
	assert.Contains(t, logs.String(), "mock-dm: rpc called")
	assert.Contains(t, logs.String(), "method=/api.dm.service.v1.ServiceService/ListClusterNodesStatus")
	assert.Contains(t, logs.String(), "code=OK")
	assert.Contains(t, logs.String(), "mock-dm: rpc failed")
	assert.Contains(t, logs.String(), "code=Unauthenticated")
	assert.Contains(t, logs.String(), "service_key validation failed")
}

// TestRunRemovesSocketOnExit 断言 dm 桩退出时移除自己的 socket 文件。
func TestRunRemovesSocketOnExit(t *testing.T) {
	address := sockPath(t)
	stop := startMock(t, address, 1, "node-1")
	assert.FileExists(t, address)
	assert.NoError(t, stop())
	assert.NoFileExists(t, address)
}

// TestListen 断言监听对路径上已有文件的处理：只清理无人监听的残留 socket；
// 仍有实例在监听、或是普通文件时不动它并报错。
func TestListen(t *testing.T) {
	t.Run("残留 socket 清理后可监听", func(t *testing.T) {
		address := sockPath(t)
		// 关闭时不摘除 socket 文件，模拟上次进程被强杀留下的残留
		stale, err := net.Listen("unix", address)
		if err != nil {
			t.Fatal(err)
		}
		stale.(*net.UnixListener).SetUnlinkOnClose(false)
		_ = stale.Close()
		assert.FileExists(t, address)

		lis, err := listen(address)
		if assert.NoError(t, err) {
			_ = lis.Close()
		}
	})
	t.Run("仍有实例在监听时不抢占", func(t *testing.T) {
		address := sockPath(t)
		other, err := net.Listen("unix", address)
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close()

		_, err = listen(address)
		assert.ErrorIs(t, err, syscall.EADDRINUSE)
		assert.FileExists(t, address)
	})
	t.Run("普通文件不清理", func(t *testing.T) {
		address := sockPath(t)
		if err := os.WriteFile(address, []byte("keep"), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := listen(address)
		assert.Error(t, err)
		got, err := os.ReadFile(address)
		if assert.NoError(t, err) {
			assert.Equal(t, "keep", string(got))
		}
	})
	t.Run("父目录不存在时报错", func(t *testing.T) {
		_, err := listen(filepath.Join(t.TempDir(), "absent", "dm.sock"))
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
}

// TestRootCmd 断言命令装配：复用配置文件的 [dm] 段、--nodes 决定节点数，以及
// 启动前拒绝非法参数。
func TestRootCmd(t *testing.T) {
	assert.Equal(t, "1", newRootCmd().Flags().Lookup("nodes").DefValue, "--nodes 默认单节点")
	assert.Equal(t, "node-1", newRootCmd().Flags().Lookup("node-id").DefValue, "--node-id 默认首节点")

	t.Run("复用配置文件的 dm 段并指定本节点", func(t *testing.T) {
		address := sockPath(t)
		file := filepath.Join(t.TempDir(), "stateflux.conf")
		// 与 stateflux.conf 同形：mock-dm 只取 [dm] 段
		ini := fmt.Sprintf("[dm]\naddress = %s\nservice_key = %s\n", address, testKey)
		if err := os.WriteFile(file, []byte(ini), 0o644); err != nil {
			t.Fatal(err)
		}

		cmd := newRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs([]string{"--config", file, "--nodes", "2", "--node-id", "node-2"})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- cmd.ExecuteContext(ctx) }()

		waitReachable(t, address)
		rpcCtx, rpcCancel := context.WithTimeout(context.Background(), waitTimeout)
		defer rpcCancel()
		resp, err := dialClient(t, address, testKey).ListClusterNodesStatus(rpcCtx, &emptypb.Empty{})
		if assert.NoError(t, err) {
			want, err := nodeTable(2, "node-2")
			assert.NoError(t, err)
			assert.Equal(t, want.String(), resp.String())
		}

		cancel() // 模拟 SIGINT/SIGTERM：RunE 正常返回
		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(waitTimeout):
			t.Fatal("命令未随 ctx 取消退出")
		}
		assert.Contains(t, out.String(), fmt.Sprintf("mock-dm: listening on %s with 2 node(s), local node node-2", address))
		assert.Contains(t, out.String(), "mock-dm: node table: 2 node(s)")
		assert.Contains(t, out.String(), "node-2  (local)")
		assert.NoFileExists(t, address)
	})

	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"节点数必须为正": {args: []string{"--dm-address", "/tmp/x.sock", "--dm-service-key", "k", "--nodes", "0"}, want: "--nodes must be positive"},
		"本节点须在表内": {args: []string{"--dm-address", "/tmp/x.sock", "--dm-service-key", "k", "--nodes", "2", "--node-id", "node-3"}, want: "not in the generated table"},
		"dm 段必填":  {args: nil, want: "dm.address and dm.service_key must not be empty"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := newRootCmd()
			cmd.SetArgs(tc.args)
			err := cmd.Execute()
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), tc.want)
			}
		})
	}
}
