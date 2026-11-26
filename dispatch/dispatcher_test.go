package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/improvtrace/stateflux/queue"
)

// testNodes 是测试用的四个在线节点（按 ID 升序，与 matchNodes 的返回序一致）。
var testNodes = []Node{
	{Id: "node-1", Online: true, Role: "LEADER"},
	{Id: "node-2", Online: true, Role: "FOLLOWER"},
	{Id: "node-3", Online: true, Role: "FOLLOWER"},
	{Id: "node-4", Online: true, Role: "FOLLOWER"},
}

// newTestDispatcher 构造带预置节点表的发布器：表在 TTL 内直接命中缓存，
// 路由计算不触达 dm 服务。
func newTestDispatcher(nodes ...Node) *Dispatcher {
	table := make(map[string]Node, len(nodes))
	for _, n := range nodes {
		table[n.Id] = n
	}
	return &Dispatcher{nodes: &nodeTable{nodes: table, fetched: time.Now()}}
}

// ids 取节点 ID 列表，空即 nil（与 matchNodes 无候选时的返回一致）。
func ids(nodes []Node) []string {
	if len(nodes) == 0 {
		return nil
	}
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Id
	}
	return out
}

// hashPick 是哈希取模的参考实现，钉住 pickNode 的哈希契约（fnv32a 对哈希键
// 取模）：重派路由是否稳定取决于它，哈希函数或哈希输入一变，同一任务的
// 目标节点就会漂移。
func hashPick(key string, candidates []Node) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return candidates[h.Sum32()%uint32(len(candidates))].Id
}

func TestParseLabels(t *testing.T) {
	got, err := ParseLabels(" disk=ssd;zone=a; zone=b ;;")
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{"disk": {"ssd"}, "zone": {"a", "b"}}, got)

	got, err = ParseLabels(" ; ")
	assert.NoError(t, err)
	assert.Empty(t, got)

	// 缺少 = 或键、值为空都是非法约束：不能静默放弃，否则任务会被投到
	// 不满足约束的节点
	for _, s := range []string{"ssd", "=ssd", "disk=", "disk=ssd;zone"} {
		_, err := ParseLabels(s)
		assert.Error(t, err, "label %q", s)
	}
}

// TestMatchNodes 断言候选过滤：在线 + vpc 交集 + 角色包含 + 标签按键交集，
// 返回按 ID 排序。
func TestMatchNodes(t *testing.T) {
	nodes := map[string]Node{
		"node-1": {Id: "node-1", Online: true, Role: "LEADER", Vpcs: []string{"v1"},
			Labels: map[string][]string{"disk": {"ssd"}, "zone": {"a", "b"}}},
		"node-2": {Id: "node-2", Online: true, Role: "FOLLOWER", Vpcs: []string{"v2"},
			Labels: map[string][]string{"disk": {"hdd"}, "zone": {"b"}}},
		"node-3": {Id: "node-3", Online: false, Role: "LEADER", Vpcs: []string{"v1"},
			Labels: map[string][]string{"disk": {"ssd"}}},
		"node-4": {Id: "node-4", Online: true, Role: "FOLLOWER"},
	}
	for _, tc := range []struct {
		name   string
		vpcs   []string
		roles  []string
		labels map[string][]string
		want   []string
	}{
		{name: "无约束取全部在线节点", want: []string{"node-1", "node-2", "node-4"}},
		{name: "vpc 交集，未声明 vpc 的节点排除", vpcs: []string{"v1"}, want: []string{"node-1"}},
		{name: "vpc 多值任一命中", vpcs: []string{"v1", "v2"}, want: []string{"node-1", "node-2"}},
		{name: "vpc 约束无节点满足", vpcs: []string{"v9"}},
		{name: "角色，离线节点不计", roles: []string{"LEADER"}, want: []string{"node-1"}},
		{name: "标签：值须命中", labels: map[string][]string{"disk": {"ssd"}}, want: []string{"node-1"}},
		{name: "标签：同键多值任一命中", labels: map[string][]string{"disk": {"ssd", "hdd"}}, want: []string{"node-1", "node-2"}},
		{name: "标签：多键须同时满足", labels: map[string][]string{"disk": {"ssd"}, "zone": {"b"}}, want: []string{"node-1"}},
		{name: "标签：未声明的键不匹配", labels: map[string][]string{"gpu": {"true"}}},
		{name: "组合约束", vpcs: []string{"v1", "v2"}, roles: []string{"FOLLOWER"},
			labels: map[string][]string{"zone": {"b"}}, want: []string{"node-2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := matchNodes(nodes, tc.vpcs, tc.roles, tc.labels)
			assert.Equal(t, tc.want, ids(got))
		})
	}
}

func TestPickNode(t *testing.T) {
	candidates := testNodes
	pick := func(uuid string, headers queue.Headers) string {
		return pickNode(&queue.Signature{UUID: uuid, Headers: headers}, candidates)
	}

	t.Run("哈希键：桶优先，缺省 UUID", func(t *testing.T) {
		assert.Equal(t, hashPick("task_1", candidates), pick("task_1", nil))
		assert.Equal(t, hashPick("42", candidates), pick("task_1", queue.Headers{AttributeHashBucket: "42"}))
		// 空桶视同未设置
		assert.Equal(t, hashPick("task_1", candidates), pick("task_1", queue.Headers{AttributeHashBucket: ""}))
	})

	t.Run("未设置偏好时按 UUID 散列，同一任务重派稳定且批次内散列", func(t *testing.T) {
		hit := map[string]int{}
		for i := 0; i < 400; i++ {
			uuid := fmt.Sprintf("task_%d", i)
			got := pick(uuid, nil)
			assert.Equal(t, got, pick(uuid, nil), "UUID %s 重派路由应稳定", uuid)
			hit[got]++
		}
		assert.Len(t, hit, len(candidates), "批次内任务应散列到全部候选节点: %v", hit)
	})

	t.Run("同桶任务无论 UUID 一律落到同一节点", func(t *testing.T) {
		hit := map[string]int{}
		for bucket := 1; bucket <= 64; bucket++ {
			headers := queue.Headers{AttributeHashBucket: fmt.Sprint(bucket)}
			want := pick("任意 UUID", headers)
			hit[want]++
			for i := 0; i < 5; i++ {
				assert.Equal(t, want, pick(fmt.Sprintf("task_%d_%d", bucket, i), headers), "桶 %d", bucket)
			}
		}
		assert.Len(t, hit, len(candidates), "不同桶应散列到全部候选节点: %v", hit)
	})

	t.Run("上次选择的节点优先于指定节点与哈希", func(t *testing.T) {
		got := pick("task_1", queue.Headers{
			AttributeClaimedNode: "node-3",
			AttributeTargetNode:  "node-2",
			AttributeHashBucket:  "7",
		})
		assert.Equal(t, "node-3", got)
	})

	t.Run("上次选择的节点不在候选内时退到指定节点", func(t *testing.T) {
		got := pick("task_1", queue.Headers{
			AttributeClaimedNode: "node-gone",
			AttributeTargetNode:  "node-2",
			AttributeHashBucket:  "7",
		})
		assert.Equal(t, "node-2", got)
	})

	t.Run("指定节点优先于哈希", func(t *testing.T) {
		for bucket := 0; bucket < 50; bucket++ {
			got := pick("task_1", queue.Headers{
				AttributeTargetNode: "node-4",
				AttributeHashBucket: fmt.Sprint(bucket),
			})
			assert.Equal(t, "node-4", got, "桶 %d", bucket)
		}
	})

	t.Run("偏好节点都不在候选内时按哈希", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			uuid := fmt.Sprintf("task_%d", i)
			got := pick(uuid, queue.Headers{
				AttributeClaimedNode: "node-gone",
				AttributeTargetNode:  "node-away",
				AttributeHashBucket:  "3",
			})
			assert.Equal(t, hashPick("3", candidates), got)
		}
	})
}

// TestAttachRoutingKey 断言完整路由计算：约束先过滤候选，再在候选内按偏好
// 或哈希选定，写入 queue:<resource>/<node>。
func TestAttachRoutingKey(t *testing.T) {
	ctx := context.Background()
	claimable := []Node{
		{Id: "node-1", Online: true},
		{Id: "node-2", Online: true, Labels: map[string][]string{"disk": {"ssd"}}},
		{Id: "node-3", Online: false, Labels: map[string][]string{"disk": {"ssd"}}},
	}
	route := func(t *testing.T, d *Dispatcher, sig *queue.Signature) string {
		t.Helper()
		if err := d.attachRoutingKey(ctx, sig); err != nil {
			t.Fatalf("attachRoutingKey: %v", err)
		}
		return sig.RoutingKey
	}

	t.Run("路由资源名取 AttributeResource，缺省取 Signature.Name", func(t *testing.T) {
		d := newTestDispatcher(testNodes...)
		assert.Equal(t, QueueName("echo", hashPick("task_1", testNodes)),
			route(t, d, &queue.Signature{UUID: "task_1", Name: "echo"}))
		assert.Equal(t, QueueName("other", hashPick("task_1", testNodes)),
			route(t, d, &queue.Signature{UUID: "task_1", Name: "echo",
				Headers: queue.Headers{AttributeResource: "other"}}))
	})

	t.Run("偏好节点被约束排除出候选时不被选用", func(t *testing.T) {
		// node-1 是偏好节点但没有 ssd 标签：标签约束把它排除出候选
		d := newTestDispatcher(claimable...)
		got := route(t, d, &queue.Signature{UUID: "task_1", Name: "echo", Headers: queue.Headers{
			AttributeClaimedNode: "node-1",
			AttributeLabels:      map[string][]string{"disk": {"ssd"}},
		}})
		assert.Equal(t, QueueName("echo", "node-2"), got)
	})

	t.Run("无候选即拒绝发布", func(t *testing.T) {
		d := newTestDispatcher(claimable...)
		err := d.attachRoutingKey(ctx, &queue.Signature{UUID: "task_1", Name: "echo", Headers: queue.Headers{
			AttributeLabels: map[string][]string{"disk": {"nvme"}},
		}})
		assert.ErrorContains(t, err, "no matching execution node")
	})

	t.Run("标签 header 类型不符即拒绝发布", func(t *testing.T) {
		// 静默忽略会丢掉约束、把任务投到不满足约束的节点
		d := newTestDispatcher(claimable...)
		sig := &queue.Signature{UUID: "task_1", Name: "echo",
			Headers: queue.Headers{AttributeLabels: "disk=ssd"}}
		err := d.attachRoutingKey(ctx, sig)
		assert.ErrorContains(t, err, AttributeLabels)
		assert.Empty(t, sig.RoutingKey)
	})
}

// TestLabelsHeaderJSONRoundTrip 断言标签 header（非字符串值）经发布时的
// JSON 序列化后仍在，消费侧反序列化不报错。
func TestLabelsHeaderJSONRoundTrip(t *testing.T) {
	sig := &queue.Signature{UUID: "task_1", Name: "echo", Headers: queue.Headers{
		AttributeLabels: map[string][]string{"disk": {"ssd", "nvme"}},
	}}
	data, err := json.Marshal(sig)
	assert.NoError(t, err)

	var got queue.Signature
	assert.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, map[string]any{"disk": []any{"ssd", "nvme"}}, got.Headers[AttributeLabels])
}
