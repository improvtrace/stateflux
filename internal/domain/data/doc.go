// Package data 数据源基建：pg 连接池与 ent client、redis client 的装配。pg/redis
// 双栈在此被使用，不设显式 pg/redis 子包；ent 生成码经 make generate 产出至本包
// ent/ 子目录，不入库。
package data
