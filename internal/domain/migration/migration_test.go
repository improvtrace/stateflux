package migration

import (
	"os"
	"strings"
	"testing"
	"testing/fstest"
)

// mapFS 构造一个只含给定 sql 文件的内存 FS（模拟嵌入目录）。
func mapFS(files map[string]string) fstest.MapFS {
	out := fstest.MapFS{}
	for name, content := range files {
		out[name] = &fstest.MapFile{Data: []byte(content)}
	}
	return out
}

func TestAvailableFromOrdersByVersionHistory(t *testing.T) {
	t.Parallel()
	fsys := mapFS(map[string]string{
		"chronos.0.2_20261001.sql": "-- b",
		"chronos.0.1_20260929.sql": "-- a",
	})
	reg := map[string]GoMigration{
		"chronos.0.1_20260929": {Name: "chronos.0.1_20260929"},
	}
	got, err := availableFrom(fsys, reg, []string{"chronos.0.1", "chronos.0.2"})
	if err != nil {
		t.Fatalf("availableFrom: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Version != "chronos.0.1" || got[1].Version != "chronos.0.2" {
		t.Fatalf("顺序错误: %+v", got)
	}
	// 同名 sql + go 共存：SQL 内容与 Go 迁移都挂在同一单元上。
	if got[0].SQL != "-- a" || got[0].Go == nil || got[0].Go.Name != "chronos.0.1_20260929" {
		t.Fatalf("sql+go 合并错误: %+v", got[0])
	}
	if got[1].Go != nil {
		t.Fatalf("无 go 迁移的单元不应挂 Go: %+v", got[1])
	}
}

func TestAvailableFromRejectsUnregisteredVersion(t *testing.T) {
	t.Parallel()
	fsys := mapFS(map[string]string{"chronos.9.9_20261001.sql": "-- x"})
	if _, err := availableFrom(fsys, nil, []string{"chronos.0.1"}); err == nil ||
		!strings.Contains(err.Error(), "未登记于 build/version") {
		t.Fatalf("err = %v, want 未登记于 build/version", err)
	}
}

func TestAvailableFromRejectsBadNameAndDuplicateVersion(t *testing.T) {
	t.Parallel()
	if _, err := availableFrom(mapFS(map[string]string{"000001_init.sql": "-- x"}), nil,
		[]string{"chronos.0.1"}); err == nil || !strings.Contains(err.Error(), "约定") {
		t.Fatalf("err = %v, want 命名约定错误", err)
	}
	fsys := mapFS(map[string]string{
		"chronos.0.1_20260929.sql": "-- a",
		"chronos.0.1_20261001.sql": "-- b",
	})
	if _, err := availableFrom(fsys, nil, []string{"chronos.0.1"}); err == nil ||
		!strings.Contains(err.Error(), "多个迁移单元") {
		t.Fatalf("err = %v, want 多个迁移单元错误", err)
	}
}

// TestAvailableReadsEmbeddedFile 守护真实嵌入：合并后的 chronos.0.1 初始化迁移
// 必须被发现且带 SQL 内容。
func TestAvailableReadsEmbeddedFile(t *testing.T) {
	t.Parallel()
	got, err := Available()
	if err != nil {
		t.Fatalf("Available: %v", err)
	}
	if len(got) != 1 || got[0].Version != "chronos.0.1" {
		t.Fatalf("Available = %+v, want 单个 chronos.0.1 迁移", got)
	}
	if !strings.Contains(got[0].SQL, "CREATE TABLE IF NOT EXISTS task_pendings") {
		t.Fatalf("合并后的初始化 SQL 内容缺失")
	}
	if !strings.Contains(got[0].Name, "chronos.0.1_") {
		t.Fatalf("迁移名 = %q, want ${version}_${date} 命名", got[0].Name)
	}
}

func TestGenerate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	created, err := Generate(dir, "chronos.0.2", "20261001")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(created) != 2 {
		t.Fatalf("created = %v, want 同时生成 sql+go 两个文件", created)
	}
	wantNames := []string{dir + "/chronos.0.2_20261001.sql", dir + "/chronos.0.2_20261001.go"}
	for i, w := range wantNames {
		if created[i] != w {
			t.Fatalf("created[%d] = %s, want %s", i, created[i], w)
		}
		if !strings.Contains(readFileT(t, w), "chronos.0.2_20261001") {
			t.Fatalf("%s 内容缺少迁移名", w)
		}
	}
	goSrc := readFileT(t, wantNames[1])
	for _, frag := range []string{"package migration", `Name: "chronos.0.2_20261001"`, "func upChronos0_2"} {
		if !strings.Contains(goSrc, frag) {
			t.Fatalf("go 模板缺少 %q:\n%s", frag, goSrc)
		}
	}
	// 重复生成拒绝覆盖；非法版本/日期报错。
	if _, err := Generate(dir, "chronos.0.2", "20261001"); err == nil ||
		!strings.Contains(err.Error(), "不覆盖") {
		t.Fatalf("err = %v, want 不覆盖错误", err)
	}
	if _, err := Generate(dir, "v1", "20261001"); err == nil {
		t.Fatal("非法版本格式应报错")
	}
	if _, err := Generate(dir, "chronos.0.3", "2026-10-01"); err == nil {
		t.Fatal("非法日期格式应报错")
	}
}

func readFileT(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
