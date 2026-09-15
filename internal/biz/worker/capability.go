// Package worker 实现 api/stateflux/worker/v1 的服务端业务（§15.1#7/#8）：执行节点能力注册与
// RPC（CapabilityService）、内置验密/上传能力实现，以及队列分配视图适配。
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/protobuf/proto"

	workerv1 "github.com/improvtrace/stateflux/api/stateflux/worker/v1"
	"github.com/improvtrace/stateflux/internal/obs"
	workerrt "github.com/improvtrace/stateflux/internal/worker"
)

// 内置能力名（§15.1#7 的示例能力）。
const (
	// CapabilityVerifyPassword 主机验密。
	CapabilityVerifyPassword = "host.verify_password"
	// CapabilityUploadFile 文件上传。
	CapabilityUploadFile = "file.upload"
)

// RegisterCapabilities 把内置能力注册进 workerrt.Registry（§15.1#7/#8）：
// 能力定义在 api/、实现在 biz、组织与注册在 worker。
func RegisterCapabilities(reg *workerrt.Registry, verifier CredentialVerifier, uploader FileUploader) error {
	if reg == nil {
		return errors.New("biz: nil capability registry")
	}
	if verifier == nil {
		verifier = DefaultVerifier{}
	}
	if uploader == nil {
		uploader = DefaultUploader{}
	}
	if err := reg.Register(workerrt.Func{
		D: workerrt.Descriptor{Name: CapabilityVerifyPassword, Description: "verify host credentials (ssh/tcp)"},
		F: func(ctx context.Context, req workerrt.Request) (workerrt.Response, error) {
			in := &workerv1.VerifyPasswordRequest{}
			if err := proto.Unmarshal(req.Payload, in); err != nil {
				return workerrt.Response{}, err
			}
			res, err := verifier.Verify(ctx, VerifyRequest{
				Host:     in.GetHost(),
				Port:     in.GetPort(),
				Protocol: in.GetProtocol(),
				Username: in.GetUsername(),
				Password: in.GetPassword(),
				Timeout:  durationFromMS(in.GetTimeoutMs()),
			})
			if err != nil {
				return workerrt.Response{}, err
			}
			out := &workerv1.VerifyPasswordResponse{Ok: res.OK, Message: res.Message}
			payload, err := proto.Marshal(out)
			if err != nil {
				return workerrt.Response{}, err
			}
			return workerrt.Response{Payload: payload}, nil
		},
	}); err != nil {
		return err
	}
	return reg.Register(workerrt.Func{
		D: workerrt.Descriptor{Name: CapabilityUploadFile, Description: "upload a file to a target host"},
		F: func(ctx context.Context, req workerrt.Request) (workerrt.Response, error) {
			in := &workerv1.UploadFileRequest{}
			if err := proto.Unmarshal(req.Payload, in); err != nil {
				return workerrt.Response{}, err
			}
			meta := in.GetMeta()
			n, err := uploader.Upload(ctx, UploadRequest{
				Host:           meta.GetHost(),
				Port:           meta.GetPort(),
				Username:       meta.GetUsername(),
				Password:       meta.GetPassword(),
				RemotePath:     meta.GetRemotePath(),
				Mode:           meta.GetMode(),
				ChecksumSHA256: meta.GetChecksumSha256(),
				Content:        in.GetContent(),
			})
			if err != nil {
				return workerrt.Response{}, err
			}
			out := &workerv1.UploadFileResponse{Ok: true, Written: n}
			payload, err := proto.Marshal(out)
			if err != nil {
				return workerrt.Response{}, err
			}
			return workerrt.Response{Payload: payload}, nil
		},
	})
}

// durationFromMS 把毫秒转为 time.Duration；<=0 返回 0（由实现取默认超时）。
func durationFromMS(ms int32) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// CapabilityServer 实现 worker/v1.CapabilityService（§15.1#7）：RPC 适配在本包，
// 能力组织与调用统一走 workerrt.Registry（§15.1#8）。
type CapabilityServer struct {
	workerv1.UnimplementedCapabilityServiceServer

	registry *workerrt.Registry
	metrics  *obs.Metrics
}

// NewCapabilityServer 构造能力服务端。
func NewCapabilityServer(registry *workerrt.Registry, metrics *obs.Metrics) *CapabilityServer {
	return &CapabilityServer{registry: registry, metrics: metrics}
}

// ListCapabilities 返回本节点已注册能力。
func (s *CapabilityServer) ListCapabilities(context.Context, *workerv1.ListCapabilitiesRequest) (*workerv1.ListCapabilitiesResponse, error) {
	descs := s.registry.Descriptors()
	out := &workerv1.ListCapabilitiesResponse{Capabilities: make([]*workerv1.CapabilityDescriptor, 0, len(descs))}
	for _, d := range descs {
		out.Capabilities = append(out.Capabilities, &workerv1.CapabilityDescriptor{
			Name:        d.Name,
			Description: d.Description,
			Labels:      d.Labels,
			Async:       d.Async,
		})
	}
	return out, nil
}

// Invoke 通用能力调用。
func (s *CapabilityServer) Invoke(ctx context.Context, req *workerv1.InvokeRequest) (*workerv1.InvokeResponse, error) {
	resp, err := s.invoke(ctx, req.GetName(), req.GetPayload(), req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// VerifyPassword 主机验密：经 registry 调用 host.verify_password 能力。
func (s *CapabilityServer) VerifyPassword(ctx context.Context, req *workerv1.VerifyPasswordRequest) (*workerv1.VerifyPasswordResponse, error) {
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := s.invoke(ctx, CapabilityVerifyPassword, payload, nil)
	if err != nil {
		return nil, err
	}
	out := &workerv1.VerifyPasswordResponse{}
	if err := proto.Unmarshal(resp.GetPayload(), out); err != nil {
		return nil, err
	}
	return out, nil
}

// UploadFile 文件上传（流式）：聚合分块后经 registry 调用 file.upload 能力。
func (s *CapabilityServer) UploadFile(stream workerv1.CapabilityService_UploadFileServer) error {
	var (
		meta    *workerv1.UploadFileMeta
		content []byte
	)
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if meta == nil {
			meta = msg.GetMeta()
		}
		content = append(content, msg.GetContent()...)
	}
	if meta == nil {
		return errors.New("biz: upload stream must start with meta")
	}
	payload, err := proto.Marshal(&workerv1.UploadFileRequest{Meta: meta, Content: content})
	if err != nil {
		return err
	}
	resp, err := s.invoke(stream.Context(), CapabilityUploadFile, payload, nil)
	if err != nil {
		return err
	}
	out := &workerv1.UploadFileResponse{}
	if err := proto.Unmarshal(resp.GetPayload(), out); err != nil {
		return err
	}
	return stream.SendAndClose(out)
}

func (s *CapabilityServer) invoke(ctx context.Context, name string, payload []byte, meta map[string]string) (*workerv1.InvokeResponse, error) {
	resp, err := s.registry.Invoke(ctx, workerrt.Request{Name: name, Payload: payload, Metadata: meta})
	if err != nil {
		if s.metrics != nil {
			s.metrics.CapabilityInvokeRecord(ctx, name, "error", 1)
		}
		return nil, fmt.Errorf("biz: capability %s: %w", name, err)
	}
	if s.metrics != nil {
		s.metrics.CapabilityInvokeRecord(ctx, name, "ok", 1)
	}
	return &workerv1.InvokeResponse{Payload: resp.Payload, Error: resp.Error}, nil
}
