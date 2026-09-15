package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// VerifyRequest 是主机验密请求（与 worker/v1.VerifyPasswordRequest 对应）。
type VerifyRequest struct {
	Host     string
	Port     int32
	Protocol string
	Username string
	Password string
	Timeout  time.Duration
}

// VerifyResult 是验密结果。
type VerifyResult struct {
	OK      bool
	Message string
}

// CredentialVerifier 是主机验密能力的具体实现（§15.1#7）。默认实现支持 ssh 与 tcp；
// 其他协议（如 winrm）返回明确错误，由部署方注入自定义实现替换。
type CredentialVerifier interface {
	Verify(ctx context.Context, req VerifyRequest) (VerifyResult, error)
}

// DefaultVerifier 是内置验密实现。
type DefaultVerifier struct{}

// Verify 执行验密。
func (DefaultVerifier) Verify(ctx context.Context, req VerifyRequest) (VerifyResult, error) {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	switch req.Protocol {
	case "", "ssh":
		return verifySSH(ctx, req, timeout)
	case "tcp":
		return verifyTCP(ctx, req, timeout)
	default:
		return VerifyResult{OK: false, Message: fmt.Sprintf("unsupported protocol %q: inject a CredentialVerifier", req.Protocol)}, nil
	}
}

func verifySSH(ctx context.Context, req VerifyRequest, timeout time.Duration) (VerifyResult, error) {
	if req.Host == "" || req.Username == "" {
		return VerifyResult{OK: false, Message: "host and username are required"}, nil
	}
	port := req.Port
	if port <= 0 {
		port = 22
	}
	cfg := &ssh.ClientConfig{
		User:            req.Username,
		Auth:            []ssh.AuthMethod{ssh.Password(req.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 内网执行节点：主机指纹校验由部署层负责
		Timeout:         timeout,
	}
	addr := net.JoinHostPort(req.Host, fmt.Sprint(port))
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return VerifyResult{OK: false, Message: err.Error()}, nil
	}
	defer conn.Close()
	sshConn, chans, reqs, err := ssh.NewClientConn(conn, addr, cfg)
	if err != nil {
		return VerifyResult{OK: false, Message: err.Error()}, nil
	}
	client := ssh.NewClient(sshConn, chans, reqs)
	defer client.Close()
	return VerifyResult{OK: true, Message: "ssh password authentication succeeded"}, nil
}

func verifyTCP(ctx context.Context, req VerifyRequest, timeout time.Duration) (VerifyResult, error) {
	if req.Host == "" {
		return VerifyResult{OK: false, Message: "host is required"}, nil
	}
	port := req.Port
	if port <= 0 {
		return VerifyResult{OK: false, Message: "port is required for tcp protocol"}, nil
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(req.Host, fmt.Sprint(port)))
	if err != nil {
		return VerifyResult{OK: false, Message: err.Error()}, nil
	}
	_ = conn.Close()
	return VerifyResult{OK: true, Message: "tcp reachable"}, nil
}

// UploadRequest 是文件上传请求（与 worker/v1.UploadFileRequest 对应）。
type UploadRequest struct {
	Host           string
	Port           int32
	Username       string
	Password       string
	RemotePath     string
	Mode           int32
	ChecksumSHA256 string
	Content        []byte
	Timeout        time.Duration
}

// FileUploader 是文件上传能力的具体实现（§15.1#7）。
type FileUploader interface {
	Upload(ctx context.Context, req UploadRequest) (int64, error)
}

// DefaultUploader 在提供凭据时走 SFTP，否则落到本地暂存目录（便于单机开发与测试）。
type DefaultUploader struct {
	// StagingRoot 是本地暂存根目录；空则用 os.TempDir()/stateflux-uploads。
	StagingRoot string
}

// Upload 执行上传并校验 sha256。
func (u DefaultUploader) Upload(ctx context.Context, req UploadRequest) (int64, error) {
	if req.ChecksumSHA256 != "" {
		sum := sha256.Sum256(req.Content)
		if got := hex.EncodeToString(sum[:]); got != req.ChecksumSHA256 {
			return 0, fmt.Errorf("biz: checksum mismatch: want %s got %s", req.ChecksumSHA256, got)
		}
	}
	if req.Username != "" {
		return u.uploadSFTP(ctx, req)
	}
	return u.uploadLocal(req)
}

func (u DefaultUploader) uploadSFTP(_ context.Context, req UploadRequest) (int64, error) {
	if req.Host == "" || req.RemotePath == "" {
		return 0, errors.New("biz: host and remote_path are required")
	}
	port := req.Port
	if port <= 0 {
		port = 22
	}
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cfg := &ssh.ClientConfig{
		User:            req.Username,
		Auth:            []ssh.AuthMethod{ssh.Password(req.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}
	client, err := ssh.Dial("tcp", net.JoinHostPort(req.Host, fmt.Sprint(port)), cfg)
	if err != nil {
		return 0, fmt.Errorf("biz: sftp dial: %w", err)
	}
	defer client.Close()
	sc, err := sftp.NewClient(client)
	if err != nil {
		return 0, fmt.Errorf("biz: sftp client: %w", err)
	}
	defer sc.Close()
	if dir := filepath.Dir(req.RemotePath); dir != "" && dir != "." {
		_ = sc.MkdirAll(dir)
	}
	f, err := sc.Create(req.RemotePath)
	if err != nil {
		return 0, fmt.Errorf("biz: sftp create: %w", err)
	}
	n, err := f.Write(req.Content)
	if err != nil {
		_ = f.Close()
		return int64(n), fmt.Errorf("biz: sftp write: %w", err)
	}
	if err := f.Close(); err != nil {
		return int64(n), err
	}
	if req.Mode != 0 {
		_ = sc.Chmod(req.RemotePath, os.FileMode(req.Mode))
	}
	return int64(n), nil
}

func (u DefaultUploader) uploadLocal(req UploadRequest) (int64, error) {
	root := u.StagingRoot
	if root == "" {
		root = filepath.Join(os.TempDir(), "stateflux-uploads")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return 0, err
	}
	name := filepath.Base(req.RemotePath)
	if name == "" || name == "." || name == "/" {
		name = "upload.bin"
	}
	path := filepath.Join(root, name)
	mode := os.FileMode(req.Mode)
	if mode == 0 {
		mode = 0o644
	}
	if err := os.WriteFile(path, req.Content, mode); err != nil {
		return 0, err
	}
	return int64(len(req.Content)), nil
}
