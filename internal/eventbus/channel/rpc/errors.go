package rpc

import "errors"

// ErrNoTarget 表示信封未指定目标节点/地址：节点寻址通道（unary / stream）必须显式给出
// 目标，否则无法调用。
var ErrNoTarget = errors.New("eventbus/channel/rpc: envelope target is empty")

// ErrClosedBeforeAck 表示事件已发出但在等到对应 ack 前流被关闭（对端断连/本端重建）。
// 该事件必须视为未确认，由调用方（worker WAL 重发 / R1）负责收敛。
var ErrClosedBeforeAck = errors.New("eventbus/channel/rpc: stream closed before ack")

