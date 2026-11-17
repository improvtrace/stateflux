package migration

import (
	"strings"
	"testing"
)

// 重建模式的 DROP 清单必须逐一列出本产品自有表，且默认不含他人可能同名的
// 裸名 version（仅内容确属本产品时由调用方追加）。
func TestOwnedDropSQL(t *testing.T) {
	got := ownedDropSQL("public", ownedTables)
	wantPrefix := "DROP TABLE IF EXISTS public.task_pendings"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("ownedDropSQL = %q, want prefix %q", got, wantPrefix)
	}
	for _, tbl := range ownedTables {
		if !strings.Contains(got, "public."+tbl) {
			t.Fatalf("ownedDropSQL = %q, missing owned table %s", got, tbl)
		}
	}
	if !strings.HasSuffix(got, " CASCADE") {
		t.Fatalf("ownedDropSQL = %q, want CASCADE suffix", got)
	}
	// 裸名 version 不在默认清单里。
	if strings.Contains(got, "public.version,") || strings.HasSuffix(got, "public.version CASCADE") {
		t.Fatalf("ownedDropSQL = %q, bare legacy version must not be dropped by default", got)
	}
}

func TestOwnedDropSQLWithLegacyVersion(t *testing.T) {
	tables := append(append([]string(nil), ownedTables...), "version")
	got := ownedDropSQL("public", tables)
	if !strings.Contains(got, "public.version") {
		t.Fatalf("ownedDropSQL = %q, want legacy version included", got)
	}
}

// 自定义 search_path 的部署同样走限定名。
func TestOwnedDropSQLCustomSchema(t *testing.T) {
	got := ownedDropSQL("sfx", ownedTables)
	if !strings.Contains(got, "sfx.task_completeds") {
		t.Fatalf("ownedDropSQL = %q, want qualified sfx.task_completeds", got)
	}
}

// 版本表名必须带前缀（共用库防撞名约定的回归锚点）。
func TestVersionTablePrefixed(t *testing.T) {
	if versionTableName != "stateflux_version" {
		t.Fatalf("versionTableName = %q, want stateflux_version", versionTableName)
	}
	if !strings.Contains(versionTableDDL, versionTableName) {
		t.Fatalf("versionTableDDL does not create %s", versionTableName)
	}
}
