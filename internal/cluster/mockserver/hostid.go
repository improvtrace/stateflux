// hostid.go —— 主机信息采集生成固定节点 ID（单机 mock 的默认身份）。
//
// 单机部署测试要求节点 ID 跨重启稳定（serve 的选举归属、任务归属都携带节点 ID），
// 硬编码常量在多台开发机上会撞名，故按「主机信息采集 + 哈希」派生：
//   - hostname 为主成分（归一为 [a-z0-9-]，保证可读）；
//   - machine-id（/etc/machine-id、/var/lib/dbus/machine-id）做机器级区分，
//     不可读时退回首个非环回网卡的 MAC；
//   - FNV-1a 32 位哈希出短后缀。
//
// 同一主机多次启动恒定；仅用标准库，读文件失败逐级回退，绝不出错。
package mockserver

import (
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"strings"
	"sync"
)

// 主机信息采集的参数。
const (
	// machineIDPaths 是 machine-id 的候选路径（按序取第一个可读的非空值）。
	machineIDPaths = "/etc/machine-id,/var/lib/dbus/machine-id"
	// maxHostLen 是 ID 中 hostname 部分的长度上限。
	maxHostLen = 24
)

// hostNodeID 计算一次并缓存：文件与网卡枚举不值得每次调用重复做。
var hostNodeID = sync.OnceValue(computeHostNodeID)

// computeHostNodeID 生成 node-<hostname>-<fnv32hex> 形态的固定节点 ID。
func computeHostNodeID() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	host = sanitizeHostname(host)
	h := fnv.New32a()
	_, _ = h.Write([]byte(host + "\x00" + machineSignature()))
	return fmt.Sprintf("node-%s-%08x", host, h.Sum32())
}

// sanitizeHostname 归一 hostname：小写、仅保留 [a-z0-9]、其余字符折叠为单个
// 「-」、限长（归一后为纯 ASCII，按字节截断安全）。
func sanitizeHostname(host string) string {
	var b strings.Builder
	lastSep := false
	for _, r := range strings.ToLower(host) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastSep = false
		default:
			if !lastSep && b.Len() > 0 {
				b.WriteByte('-')
				lastSep = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "localhost"
	}
	if len(out) > maxHostLen {
		out = strings.Trim(out[:maxHostLen], "-")
	}
	return out
}

// machineSignature 返回可区分主机的稳定签名：machine-id 优先，MAC 兜底，
// 均不可得时为空（ID 退化为仅由 hostname 决定，仍保持固定）。
func machineSignature() string {
	for _, p := range strings.Split(machineIDPaths, ",") {
		if raw, err := os.ReadFile(p); err == nil {
			if s := strings.TrimSpace(string(raw)); s != "" {
				return "machineid:" + s
			}
		}
	}
	if mac := firstHardwareMAC(); mac != "" {
		return "mac:" + mac
	}
	return ""
}

// firstHardwareMAC 返回首个非环回、已启用且具备硬件地址的网卡 MAC。
func firstHardwareMAC() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagLoopback != 0 || ifc.Flags&net.FlagUp == 0 {
			continue
		}
		if mac := ifc.HardwareAddr.String(); mac != "" {
			return mac
		}
	}
	return ""
}
