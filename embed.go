// Package stateflux 是模块根包，仅承载编译期嵌入的仓库级构建资产。
// go:embed 只能引用包目录内的相对路径，而版本历史文件 build/version 位于仓库根
// build/ 下，因此嵌入指令必须放在模块根包；pkg/buildinfo 经 VersionFile 消费，
// 是唯一读取方（避免重复嵌入或复制同步）。
package stateflux

import _ "embed"

// VersionFile 是 build/version 的原始内容：每行一个历史版本
// （${alias}.${major}.${minor}，# 起始行为注释），末行为当前版本。
//
//go:embed build/version
var VersionFile []byte
