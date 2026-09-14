package schema

// 本文件定义任务表组的枚举与取值区间（§3.1）。三类字段的处理方式不同，不要混：
//
//   - `outcome` 是**封闭枚举**：以整型存储，0 保留为「未设置」，合法值从 1 开始，
//     使 Go 零值与漏赋值可检测；
//   - `priority` 是**连续区间** `[0,100]`，不是枚举：业务可按自己的语义细分（0=最低、100=最高），
//     调度侧只依赖排序、不解释具体数值；
//   - `vpc` / `node` / `label` 是**外部标识或接入方自定义的自由文本**（VPC 名、执行节点 ID、节点
//     匹配标签）：既不是枚举，也不做数值编码，取值合法性由集群视图在运行期给出（§3.1/§5.3）；
//   - `hash_bucket` 是 0–255 的整数分桶，0=不限：接入方自定义语义，框架只做等值匹配，
//     区间由 BucketCheck 钉住（§3.1/§5.2）。
//
// 编码是存储契约：改动任何数值/区间都等于数据迁移。常量与表定义同包——ent 生成码反向 import
// 本包，故本包不得 import domain，否则形成 schema → domain → domain/data/ent → schema 的环。

// Priority 是调度优先级：闭区间 [MinPriority, MaxPriority]，数值越大越优先。晋升与认领均按该列
// DESC 排序（§5.2），因此 100 最先被处理、0 最后。区间由各表的 CHECK 约束钉住
// （PriorityCheck），保证越界值写不进来。
type Priority int8

const (
	// MinPriority 区间下界：最低优先级。
	MinPriority Priority = 0
	// MaxPriority 区间上界：最高优先级。
	MaxPriority Priority = 100
	// DefaultPriority 新建任务行的默认优先级（区间中点，对应旧稿的 normal）。
	DefaultPriority Priority = 50
)

// PriorityCheck 是钉住 priority 取值区间的 CHECK 约束表达式，由各表注解注入建表 DDL（§3.1）。
const PriorityCheck = "priority >= 0 AND priority <= 100"

// BucketCheck 是钉住 hash_bucket 取值区间（0–255）的 CHECK 约束表达式，由含该列的表
// （pending/schedulable/processing）注解注入建表 DDL（§3.1/§5.2）。
const BucketCheck = "hash_bucket >= 0 AND hash_bucket <= 255"

// PriorityBand 是任务 topic 使用的优先级档位：§3.2 的 task.{band}。
//
// 为什么需要档位：priority 是 0–100 的连续值，若 topic 直接用原始数值会产生 101 个 topic，
// 而 topic 只用于订阅与投递分组，不该随业务细分膨胀。档位不替代 PG 的 priority 排序——
// 认领顺序仍由 priority 精确决定（§5.2），档位只决定消息落在哪个 topic。
type PriorityBand string

const (
	// BandLow 覆盖 priority 0–33。
	BandLow PriorityBand = "low"
	// BandNormal 覆盖 priority 34–66（含默认值 50）。
	BandNormal PriorityBand = "normal"
	// BandHigh 覆盖 priority 67–100。
	BandHigh PriorityBand = "high"
)

// BandOf 把数值 priority 映射为 topic 档位（§3.2）。区间外的值向边界收敛（不 panic：越界值本应被
// CHECK 拦住，这里只做防御性归一）。
func BandOf(p Priority) PriorityBand {
	switch {
	case p <= 33:
		return BandLow
	case p <= 66:
		return BandNormal
	default:
		return BandHigh
	}
}

// Outcome 是终态类别（§3.1、§5.5），用于 completed_tasks 与 task_results。与 priority 不同，
// 它是封闭枚举，不做区间取值。
type Outcome int8

const (
	// OutcomeUnset 是缺省零值，不是合法终态：终态事务必须显式给出。
	OutcomeUnset Outcome = 0
	// OutcomeSucceeded 执行成功。
	OutcomeSucceeded Outcome = 1
	// OutcomeFailed 执行失败且不再重试（重试次数未用尽前不进终态）。
	OutcomeFailed Outcome = 2
	// OutcomeDead 死信（R3：attempts >= max_attempts，§6.2）。
	OutcomeDead Outcome = 3
)

// String 返回可读名，用于日志与观测标签（§6.4 的 outcome 标签取值即来自此处）。
func (o Outcome) String() string {
	switch o {
	case OutcomeSucceeded:
		return "succeeded"
	case OutcomeFailed:
		return "failed"
	case OutcomeDead:
		return "dead"
	default:
		return "unset"
	}
}
