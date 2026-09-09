package executor

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"
	"github.com/improvtrace/stateflux/queue"
)

// Server 执行角色对内提供的 gRPC 服务（§7）：
//
//	Execute —— 同步任务执行（带 deadline）；
//	Collect/Ack —— 异步结果归集（pull + Ack 两段式，§5.5）。
type Server struct {
	dispatchv1.UnimplementedExecutorServiceServer

	exec   *Executor
	wal    *WAL
	q      *queue.Queue
	leases *leaseManager
	nodeID string
	lease  func() // 占位避免未使用告警
	log    *slog.Logger
}

// NewServer 构造执行角色 gRPC server。
func NewServer(exec *Executor, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		exec:   exec,
		wal:    exec.wal,
		q:      exec.q,
		leases: exec.leases,
		nodeID: exec.nodeID,
		log:    log,
	}
}

// Execute 同步任务执行（§5.4 sync）：注册/续约逻辑与异步一致；结果随 RPC 响应返回
// （调度侧归集，不经 WAL）。响应后移出 inprocess——同步任务的结果已交给调度节点的
// 统一结果缓冲，若调度节点在 flush 前丢失结果，由对账按 grace 重置（at-least-once）。
func (s *Server) Execute(ctx context.Context, req *dispatchv1.ExecuteRequest) (*dispatchv1.ExecuteResponse, error) {
	msg := req.GetTask()
	if msg == nil {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	reg, err := s.q.Register(ctx, msg.TaskId, msg.Attempt, s.nodeID, s.exec.leaseTTL)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "register inprocess: %v", err)
	}
	if !reg.OK {
		// 陈旧副本或任务已终态（墓碑）——调度节点应放弃本次等待，交对账收敛。
		return nil, status.Errorf(codes.FailedPrecondition, "stale copy rejected: %s", reg.RejectOf)
	}
	s.leases.Start(msg.TaskId, msg.Attempt)
	entry := s.exec.runHandler(ctx, msg)
	// 响应后移出 inprocess（归属校验；失败仅记录——墓碑/新尝试接管均无害）。
	if !s.leases.Stop(msg.TaskId, true) && s.exec != nil {
		s.log.Debug("execute: inprocess remove rejected", "task_id", msg.TaskId)
	}
	return &dispatchv1.ExecuteResponse{Result: entry}, nil
}

// Collect 拉取未 Ack 结果（§5.5 pull 模型；WAL 保留，等 Ack）。
func (s *Server) Collect(ctx context.Context, req *dispatchv1.CollectRequest) (*dispatchv1.CollectResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 500
	}
	entries := s.wal.Collect(limit)
	resp := &dispatchv1.CollectResponse{Results: entries}
	return resp, nil
}

// Ack 归集完成回执（§5.5）：推进 WAL 截断水位 + 移出 inprocess（归属校验）。
// Ack 丢失无害：重拉 → PG 幂等回写 → 重新 Ack（§6.1）。
func (s *Server) Ack(ctx context.Context, req *dispatchv1.AckRequest) (*dispatchv1.AckResponse, error) {
	ids := req.GetTaskIds()
	if len(ids) == 0 {
		return &dispatchv1.AckResponse{}, nil
	}
	if _, err := s.wal.Ack(ids); err != nil {
		return nil, status.Errorf(codes.Internal, "wal ack: %v", err)
	}
	for _, id := range ids {
		// 移出 inprocess：以本执行者记录的 attempt 做归属校验（僵尸节点被拒，§12.6）。
		s.leases.Stop(id, true)
	}
	s.log.Debug("ack processed", "count", len(ids))
	return &dispatchv1.AckResponse{}, nil
}

// Backlog 暴露 WAL 积压（观测/调试）。
func (s *Server) Backlog() (int64, int64) { return s.wal.Backlog() }

// String 便于日志。
func (s *Server) String() string { return fmt.Sprintf("executor-server(%s)", s.nodeID) }
