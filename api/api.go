// Package api 实现 API 角色（§8，阶段 1，所有节点常驻）：
// 模式 B 的 CreateTasks RPC（§5.1）、同步任务创建方回执长轮询 GetResults（§5.6）、
// 死信运维面（dead 查询 + redrive，§12.9）。
package api

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	apiv1 "github.com/improvtrace/stateflux/proto/gen/apiv1"

	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

// Service 业务接入点（TaskService + AdminService）。
type Service struct {
	apiv1.UnimplementedTaskServiceServer
	admin apiv1.UnimplementedAdminServiceServer

	store store.Store
	cfg   config.APIConfig
	log   *slog.Logger
}

// New 构造 API 服务。
func New(s store.Store, cfg config.APIConfig, log *slog.Logger) *Service {
	cfg.ApplyDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: s, cfg: cfg, log: log}
}

// Admin 返回死信运维服务（§12.9）。
func (s *Service) Admin() apiv1.AdminServiceServer { return &s.admin }

// CreateTasks 模式 B 创建（§5.1）：单事务批量 INSERT pending_tasks + task_payloads，
// 幂等键冲突跳过插入、返回已存在 task_id（与模式 A 业务直写幂等语义一致）。
func (s *Service) CreateTasks(ctx context.Context, req *apiv1.CreateTasksRequest) (*apiv1.CreateTasksResponse, error) {
	items := req.GetItems()
	if len(items) == 0 {
		return nil, status.Error(codes.InvalidArgument, "items is required")
	}
	tasks := make([]sdk.NewTask, 0, len(items))
	for _, item := range items {
		if item.GetType() == "" {
			return nil, status.Error(codes.InvalidArgument, "task type is required")
		}
		nt := sdk.NewTask{
			Type:           item.GetType(),
			Payload:        item.GetPayload(),
			Priority:       sdk.Priority(item.GetPriority()),
			ExecMode:       sdk.ExecMode(item.GetExecMode()),
			RunAt:          unixMS(item.GetRunAtUnixMs()),
			TimeoutMS:      item.GetTimeoutMs(),
			MaxAttempts:    item.GetMaxAttempts(),
			IdempotencyKey: item.GetIdempotencyKey(),
			BatchID:        item.GetBatchId(),
			ParentTaskID:   0,
		}
		if len(item.GetCallbackJson()) > 0 {
			spec, err := sdk.UnmarshalCallback(item.GetCallbackJson())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid callback spec: %v", err)
			}
			if err := spec.Validate(); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid callback spec: %v", err)
			}
			nt.Callback = spec
		}
		tasks = append(tasks, nt)
	}
	created, err := s.store.CreatePending(ctx, tasks)
	if err != nil {
		if errorsIs(err, store.ErrInvalidTask, store.ErrEmptyBatch) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "create tasks: %v", err)
	}
	out := &apiv1.CreateTasksResponse{Tasks: make([]*apiv1.CreatedTask, 0, len(created))}
	for _, c := range created {
		out.Tasks = append(out.Tasks, &apiv1.CreatedTask{TaskId: c.TaskID, Duplicate: c.Duplicate})
	}
	return out, nil
}

// GetResults 创建方回执（§5.6 长轮询）：先查 task_results（终态即命中返回），未命中查
// 阶段表判断在途状态后继续等待，归集 flush 后命中即返回；全部终态或到期返回当前快照。
func (s *Service) GetResults(ctx context.Context, req *apiv1.GetResultsRequest) (*apiv1.GetResultsResponse, error) {
	ids := req.GetTaskIds()
	if len(ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "task_ids is required")
	}
	wait := time.Duration(req.GetWaitMs()) * time.Millisecond
	if wait <= 0 {
		return s.snapshot(ctx, ids)
	}
	if wait > s.cfg.MaxLongPoll {
		wait = s.cfg.MaxLongPoll
	}
	deadline := time.Now().Add(wait)
	for {
		results, stages, err := s.store.GetResults(ctx, ids)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get results: %v", err)
		}
		if allTerminal(ids, results) || time.Now().After(deadline) || ctx.Err() != nil {
			return toResponse(ids, results, stages), nil
		}
		select {
		case <-ctx.Done():
			return toResponse(ids, results, stages), nil
		case <-time.After(s.cfg.LongPollInterval):
		}
	}
}

func (s *Service) snapshot(ctx context.Context, ids []int64) (*apiv1.GetResultsResponse, error) {
	results, stages, err := s.store.GetResults(ctx, ids)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get results: %v", err)
	}
	return toResponse(ids, results, stages), nil
}

func toResponse(ids []int64, results map[int64]*sdk.TaskResult, stages map[int64]sdk.Stage) *apiv1.GetResultsResponse {
	resp := &apiv1.GetResultsResponse{Results: make([]*apiv1.TaskResultItem, 0, len(ids))}
	for _, id := range ids {
		item := &apiv1.TaskResultItem{TaskId: id, Stage: string(stages[id])}
		if r := results[id]; r != nil {
			item.Stage = string(sdk.StageCompleted)
			item.Outcome = string(r.Outcome)
			item.Attempt = r.Attempt
			item.Result = r.Result
			item.Error = r.Error
			item.CompletedAtUnixMs = r.CompletedAt.UnixMilli()
		}
		resp.Results = append(resp.Results, item)
	}
	return resp
}

func allTerminal(ids []int64, results map[int64]*sdk.TaskResult) bool {
	for _, id := range ids {
		if results[id] == nil {
			return false
		}
	}
	return true
}

// ---- AdminService（§12.9 死信运维面） ----

// ListDeadTasks dead 查询：按类型过滤、id 倒序分页。
func (s *Service) ListDeadTasks(ctx context.Context, req *apiv1.ListDeadTasksRequest) (*apiv1.ListDeadTasksResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	tasks, next, err := s.store.ListDead(ctx, req.GetType(), limit, req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dead: %v", err)
	}
	resp := &apiv1.ListDeadTasksResponse{NextCursor: next, Tasks: make([]*apiv1.DeadTask, 0, len(tasks))}
	for _, t := range tasks {
		resp.Tasks = append(resp.Tasks, &apiv1.DeadTask{
			TaskId:            t.Task.ID,
			Type:              t.Task.Type,
			Payload:           t.Payload,
			Attempts:          int32(t.Task.Attempts),
			MaxAttempts:       t.Task.MaxAttempts,
			Error:             t.Task.Error,
			CreatedAtUnixMs:   t.Task.CreatedAt.UnixMilli(),
			CompletedAtUnixMs: t.CompletedAt.UnixMilli(),
		})
	}
	return resp, nil
}

// RedriveTasks 死信重跑：人工修复后重跑（复制原 payload 创建全新任务实例，
// 新雪花 ID、attempts 归零，不自动重放）。
func (s *Service) RedriveTasks(ctx context.Context, req *apiv1.RedriveTasksRequest) (*apiv1.RedriveTasksResponse, error) {
	ids := req.GetTaskIds()
	if len(ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "task_ids is required")
	}
	newIDs, err := s.store.Redrive(ctx, ids)
	if err != nil {
		if errorsIs(err, store.ErrNotFound, store.ErrEmptyBatch) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "redrive: %v", err)
	}
	return &apiv1.RedriveTasksResponse{NewTaskIds: newIDs}, nil
}

// Ping 存活探测。
func (s *Service) Ping(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// ---- 小工具 ----

func unixMS(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// errorsIs 任一目标匹配即 true。
func errorsIs(err error, targets ...error) bool {
	for _, t := range targets {
		if err == t {
			return true
		}
	}
	return false
}
