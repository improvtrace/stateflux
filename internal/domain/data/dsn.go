package data

import "strings"

// DefaultSearchPath 是未显式配置时固定的 search_path（§3.1）。
const DefaultSearchPath = "public"

func defaultSearchPath(schema string) string {
	if schema == "" {
		return DefaultSearchPath
	}
	return schema
}

// WithSearchPath 在 DSN 上追加 search_path 连接参数（幂等）。PostgreSQL 默认 search_path 为
// `"$user", public`：当数据库用户名与某个 schema（如本项目的 stateflux）同名时，表和函数会
// 被解析到该 schema，导致应用与 SQL 函数写入不同的表组（§3.1）。显式固定 search_path 消除歧义。
//
// 同时支持 URL 形式（postgres://...?a=b）与 keyword/value 形式（host=... dbname=...）。
func WithSearchPath(dsn, schema string) string {
	if schema == "" || dsn == "" {
		return dsn
	}
	if hasSearchPath(dsn) {
		return dsn
	}
	if strings.Contains(dsn, "://") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		return dsn + sep + "search_path=" + schema
	}
	return strings.TrimSpace(dsn) + " search_path=" + schema
}

func hasSearchPath(dsn string) bool {
	lower := strings.ToLower(dsn)
	return strings.Contains(lower, "search_path=") || strings.Contains(lower, "search_path%3d")
}
