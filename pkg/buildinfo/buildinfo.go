// Package buildinfo 承载构建信息：
//   - version：来自嵌入的 build/version（根包 embed.go，每行一个历史版本，末行为
//     当前版本）——它是版本的唯一事实来源，产物命名（dist/package、dist/image）
//     与二进制报告天然一致，无需注入；
//   - commit / builddate：经 -ldflags "-X github.com/improvtrace/stateflux/pkg/buildinfo.<Field>=..."
//     注入（Makefile LDFLAGS 集中维护，make build / install / docker 与 Docker 镜像
//     内构建共用同一来源）；
//   - goversion：运行时取 runtime.Version()。
//
// 未注入时 commit / builddate 为 unknown 占位。
package buildinfo

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/improvtrace/stateflux"
)

var (
	// Commit 构建对应 commit 短哈希，编译注入。
	Commit = "unknown"
	// Date 构建时间（UTC，RFC3339），编译注入。
	Date = "unknown"
)

// GoVersion 返回构建所用 Go 工具链版本（如 go1.27.0）。
func GoVersion() string { return runtime.Version() }

// Versions 返回 build/version 的全部历史版本（文件顺序，旧→新）；
// 空行与 # 注释行忽略。迁移执行顺序（internal/domain/migration）以此为序。
func Versions() []string {
	out := make([]string, 0, 8)
	for _, line := range strings.Split(string(stateflux.VersionFile), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}

// Version 返回当前版本（历史末行，如 chronos.0.1）；文件缺失或为空时回退 dev。
func Version() string {
	if vs := Versions(); len(vs) > 0 {
		return vs[len(vs)-1]
	}
	return "dev"
}

// String 返回单行完整构建信息；stateflux --version 与 stateflux info 共用本格式。
func String() string {
	return fmt.Sprintf("version=%s commit=%s builddate=%s goversion=%s",
		Version(), Commit, Date, GoVersion())
}
