// Package task 实现 api/stateflux/task/v1 的服务端业务（§15.1#2）：Execute 同步执行编排、
// ResultStream/Collect 结果归集、结果发布适配、工厂入队与内置演示工厂/handler。
package task

import (
	"context"
	"errors"
	"io"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/obs"
	"github.com/improvtrace/stateflux/internal/worker"
)

// ResultConsumer 消费一条 ResultEvent（由 Collector 实现，§5.5；接口在此声明以避免
// biz → runtime 的反向依赖）。
type ResultConsumer interface {
	Consume(ctx context.Context, ev *taskv1.ResultEvent) error
}

// ExecutorServer 实现 task/v1.ExecutorService（§7、§15.1#2）：
//   - Execute 是同步 request/reply 入口，直接调用 worker 运行时执行；
//   - ResultStream 是默认结果归集通道，worker 发布、本服务订阅并转交 Collector；
//   - Collect 是同步批量结果上报入口。
type ExecutorServer struct {
	taskv1.UnimplementedExecutorServiceServer

	runtime *worker.Runtime
	results ResultConsumer
	metrics *obs.Metrics
}

// NewExecutorServer 构造执行接入服务端。
func NewExecutorServer(runtime *worker.Runtime, results ResultConsumer, metrics *obs.Metrics) *ExecutorServer {
	return &ExecutorServer{runtime: runtime, results: results, metrics: metrics}
}

// Execute 同步执行一个任务并返回 ResultEvent。
func (s *ExecutorServer) Execute(ctx context.Context, req *taskv1.ExecuteRequest) (*taskv1.ExecuteResponse, error) {
	if req.GetTask() == nil {
		return nil, errors.New("biz: execute requires a task message")
	}
	if s.runtime == nil {
		return nil, errors.New("biz: executor runtime is not configured")
	}
	ev := s.runtime.Execute(ctx, req.GetTask())
	// 同步路径的结果也走同一个归集入口（§5.5）：RPC 返回与异步发布不分成两套机制。
	if s.results != nil {
		_ = s.results.Consume(ctx, ev)
	}
	return &taskv1.ExecuteResponse{Result: ev}, nil
}

// ResultStream 订阅 worker 的结果流并逐条归集；每条回 ResultAck（仅表示已尝试归集）。
func (s *ExecutorServer) ResultStream(stream taskv1.ExecutorService_ResultStreamServer) error {
	if s.results == nil {
		return errors.New("biz: no result consumer configured")
	}
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		accepted := true
		message := ""
		if cerr := s.results.Consume(stream.Context(), ev); cerr != nil {
			accepted = false
			message = cerr.Error()
		}
		if err := stream.Send(&taskv1.ResultAck{
			Accepted: accepted,
			Message:  message,
			TaskId:   ev.GetTaskId(),
			Attempt:  ev.GetAttempt(),
		}); err != nil {
			return err
		}
	}
}

// Collect 同步批量归集。
func (s *ExecutorServer) Collect(ctx context.Context, req *taskv1.CollectRequest) (*taskv1.CollectResponse, error) {
	resp := &taskv1.CollectResponse{}
	for _, ev := range req.GetResults() {
		if s.results == nil {
			resp.Rejected++
			continue
		}
		if err := s.results.Consume(ctx, ev); err != nil {
			resp.Rejected++
			continue
		}
		resp.Accepted++
	}
	if s.metrics != nil {
		s.metrics.CollectorResultRecord(ctx, "succeeded", int64(resp.Accepted))
	}
	return resp, nil
}
