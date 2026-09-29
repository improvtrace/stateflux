package buildinfo

import (
	"regexp"
	"strings"
	"testing"
)

// versionRe 校验 build/version 的版本格式：${alias}.${major}.${minor}。
var versionRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*\.[0-9]+\.[0-9]+$`)

// TestVersionsFromEmbeddedFile 守护嵌入的版本历史：至少一个版本、全部符合
// ${alias}.${major}.${minor} 格式，Version() 为末行。
func TestVersionsFromEmbeddedFile(t *testing.T) {
	vs := Versions()
	if len(vs) == 0 {
		t.Fatal("Versions() 为空：build/version 未嵌入或没有有效版本行")
	}
	for _, v := range vs {
		if !versionRe.MatchString(v) {
			t.Fatalf("版本 %q 不符合 ${alias}.${major}.${minor} 格式", v)
		}
	}
	if got, want := Version(), vs[len(vs)-1]; got != want {
		t.Fatalf("Version() = %q, want 历史末行 %q", got, want)
	}
}

// TestStringContainsAllFields 守护 --version / info 共用的单行格式：
// 四个构建信息字段都必须出现（未注入时为 unknown 占位，仍保持完整结构）。
func TestStringContainsAllFields(t *testing.T) {
	s := String()
	for _, field := range []string{"version=", "commit=", "builddate=", "goversion="} {
		if !strings.Contains(s, field) {
			t.Fatalf("String() = %q, missing field %q", s, field)
		}
	}
}
