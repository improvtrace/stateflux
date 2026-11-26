package metrics

import (
	"strings"
	"testing"
)

// TestCatalogIsWellFormed 校验指标契约的硬性约束：命名前缀、唯一性、类型与
// 后缀、单位、Help 文案、标签与优先级。任何新增指标都必须通过这些规则。
func TestCatalogIsWellFormed(t *testing.T) {
	knownLabels := map[string]bool{
		LabelNode: true, LabelResource: true, LabelQueue: true, LabelHandler: true,
		LabelTrigger: true, LabelResult: true, LabelReason: true, LabelStage: true,
		LabelOutcome: true, LabelOperation: true, LabelMethod: true, LabelState: true,
		LabelEvent: true, LabelKind: true,
	}
	knownComponents := map[string]bool{
		ComponentScheduler: true, ComponentPublisher: true, ComponentDispatch: true,
		ComponentReconciler: true, ComponentExecution: true, ComponentState: true,
		ComponentRuntime: true,
	}

	seen := make(map[string]bool)
	for _, m := range Catalog() {
		if !strings.HasPrefix(m.Name, Namespace+"_") {
			t.Errorf("%s: name must start with %q", m.Name, Namespace+"_")
		}
		if seen[m.Name] {
			t.Errorf("%s: duplicate metric name", m.Name)
		}
		seen[m.Name] = true

		switch m.Kind {
		case Counter:
			if !strings.HasSuffix(m.Name, "_total") {
				t.Errorf("%s: counter names must end with _total", m.Name)
			}
		case Gauge:
			if strings.HasSuffix(m.Name, "_total") {
				t.Errorf("%s: gauge names must not end with _total", m.Name)
			}
		case Histogram:
			if strings.HasSuffix(m.Name, "_total") {
				t.Errorf("%s: histogram names must not end with _total", m.Name)
			}
		default:
			t.Errorf("%s: unknown kind %q", m.Name, m.Kind)
		}

		if m.Unit == "" {
			t.Errorf("%s: unit is required", m.Name)
		}
		if m.Help == "" || !strings.HasSuffix(m.Help, ".") {
			t.Errorf("%s: help must be a non-empty sentence ending with a period", m.Name)
		}
		if !knownComponents[m.Component] {
			t.Errorf("%s: unknown component %q", m.Name, m.Component)
		}
		switch m.Priority {
		case P0, P1, P2:
		default:
			t.Errorf("%s: unknown priority %q", m.Name, m.Priority)
		}

		labels := make(map[string]bool, len(m.Labels))
		for _, label := range m.Labels {
			if !knownLabels[label] {
				t.Errorf("%s: unknown label %q", m.Name, label)
			}
			if labels[label] {
				t.Errorf("%s: duplicate label %q", m.Name, label)
			}
			labels[label] = true
		}
	}
}

// TestCatalogGet 校验按名查询与副本语义。
func TestCatalogGet(t *testing.T) {
	m, ok := Get(CronTicksTotal)
	if !ok {
		t.Fatalf("Get(%q): not found", CronTicksTotal)
	}
	if m.Kind != Counter || m.Component != ComponentScheduler {
		t.Fatalf("Get(%q) = %+v, want counter/scheduler", CronTicksTotal, m)
	}
	if _, ok := Get(Namespace + "_does_not_exist"); ok {
		t.Fatal("Get: unknown metric should not be found")
	}

	// Catalog 返回副本：调用方修改（含标签切片）不影响包内定义。
	got := Catalog()
	got[0].Name = "mutated"
	if again, _ := Get(catalog[0].Name); again.Name == "mutated" {
		t.Fatal("Catalog must return a copy")
	}
	var ticks Metric
	for _, m := range Catalog() {
		if m.Name == CronTicksTotal {
			ticks = m
			break
		}
	}
	if len(ticks.Labels) == 0 {
		t.Fatal("CronTicksTotal must have at least one label")
	}
	ticks.Labels[0] = "mutated"
	if again, _ := Get(CronTicksTotal); again.Labels[0] == "mutated" {
		t.Fatal("Catalog must deep-copy label slices")
	}
}

// TestCatalogCoversCoreLifecycle 校验核心链路（触发、发布、路由、执行、对账）
// 都有 P0 指标，避免契约遗漏关键环节。
func TestCatalogCoversCoreLifecycle(t *testing.T) {
	required := []string{
		CronTicksTotal,
		CronLockErrorsTotal,
		CronEventsDroppedTotal,
		PublishTasksTotal,
		PublishRowsDispatchedTotal,
		PublishErrorsTotal,
		DispatchAttemptsTotal,
		DispatchErrorsTotal,
		ReconcileRoundsTotal,
		ReconcileErrorsTotal,
		TaskExecutionsTotal,
		TaskWritebackErrorsTotal,
		QueueMessagesProcessedTotal,
		QueuePanicsTotal,
		DBOperationsTotal,
		LedgerOperationsTotal,
		Up,
		PostgresUp,
		RedisUp,
	}
	for _, name := range required {
		m, ok := Get(name)
		if !ok {
			t.Errorf("%s: core metric missing from catalog", name)
			continue
		}
		if m.Priority != P0 {
			t.Errorf("%s: core metric must be P0, got %s", name, m.Priority)
		}
	}
}
