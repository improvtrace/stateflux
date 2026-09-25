package task

import (
	"testing"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"google.golang.org/protobuf/proto"
)

func TestSignatureOf(t *testing.T) {
	if SignatureOf("demo", "heartbeat") != Signature("demo:heartbeat") {
		t.Fatal("signature must be type:operator")
	}
}

func TestFuncTaskImplementsTask(t *testing.T) {
	var _ Task = FuncTask{S: Spec{Type: "t"}, P: []byte("{}")}
	spec := Spec{Type: "demo", Operator: "beat"}
	ft := FuncTask{S: spec, P: []byte("{}")}
	if ft.Spec().Type != "demo" || string(ft.Payload()) != "{}" {
		t.Fatalf("func task = %+v", ft)
	}
}

func TestRegistryRegisterAndResolve(t *testing.T) {
	reg := NewRegistry()
	sig, err := reg.Register(func() proto.Message { return &taskv1.TaskMessage{} })
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if sig != Signature(proto.MessageName(&taskv1.TaskMessage{})) {
		t.Fatalf("sig = %q", sig)
	}
	if !reg.Has(sig) {
		t.Fatal("registered signature must be found")
	}
	msg, err := reg.New(sig)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, ok := msg.(*taskv1.TaskMessage); !ok {
		t.Fatalf("new returned %T", msg)
	}

	sigs := reg.Signatures()
	if len(sigs) != 1 || sigs[0] != sig {
		t.Fatalf("signatures = %v", sigs)
	}
}

func TestRegistryRegisterValidation(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(nil); err == nil {
		t.Fatal("nil constructor must be rejected")
	}
	sig, err := reg.Register(func() proto.Message { return &taskv1.TaskMessage{} })
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := reg.Register(func() proto.Message { return &taskv1.TaskMessage{} }); err == nil {
		t.Fatal("duplicate signature must be rejected")
	}
	if reg.Has(sig + "x") {
		t.Fatal("unknown signature must not resolve")
	}
	if _, err := reg.New(sig + "x"); err == nil {
		t.Fatal("new on unregistered signature must fail")
	}
}
