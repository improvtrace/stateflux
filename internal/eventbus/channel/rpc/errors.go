package rpc

import "errors"

// ErrNoTarget 表示信封未指定目标节点/地址：节点寻址通道（unary / stream）必须显式给出
// 目标，否则无法调用。
var ErrNoTarget = errors.New("eventbus/channel/rpc: envelope target is empty")
