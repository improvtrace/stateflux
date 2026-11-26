package queue

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	opentracing "github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	opentracing_log "github.com/opentracing/opentracing-go/log"
)

// ErrTaskPanicked is returned when invoking a task causes a panic.
var ErrTaskPanicked = errors.New("Invoking task caused a panic")

// Task wraps a signature and methods used to reflect task arguments and
// return values after invoking the task
type Task struct {
	TaskFunc   reflect.Value
	UseContext bool
	Context    context.Context
	Args       []reflect.Value
}

type signatureCtxType struct{}

var signatureCtx signatureCtxType

// SignatureFromContext gets the signature from the context
func SignatureFromContext(ctx context.Context) *Signature {
	if ctx == nil {
		return nil
	}

	v := ctx.Value(signatureCtx)
	if v == nil {
		return nil
	}

	signature, _ := v.(*Signature)
	return signature
}

// NewWithSignature is the same as New but injects the signature
func NewWithSignature(taskFunc any, signature *Signature) (*Task, error) {
	if signature == nil {
		return nil, ErrInvalidArgument
	}
	if reflect.ValueOf(taskFunc).Kind() != reflect.Func {
		return nil, ErrTaskMustBeFunc
	}

	args := signature.Args
	ctx := context.Background()
	ctx = context.WithValue(ctx, signatureCtx, signature)
	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  ctx,
	}

	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %w", err)
	}

	return task, nil
}

// NewTask tries to use reflection to convert the function and arguments
// into a reflect.Value and prepare it for invocation
func NewTask(taskFunc any, args []Arg) (*Task, error) {
	// reflect.TypeOf(taskFunc).NumIn() below would panic on a nil or
	// non-func task, so validate the func kind first (nil included)
	if reflect.ValueOf(taskFunc).Kind() != reflect.Func {
		return nil, ErrTaskMustBeFunc
	}

	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  context.Background(),
	}

	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %w", err)
	}

	return task, nil
}

// Call attempts to call the task with the supplied arguments.
//
// `err` is set in the return value in two cases:
//  1. The reflected function invocation panics (e.g. due to a mismatched
//     argument list).
//  2. The task func itself returns a non-nil error.
func (t *Task) Call() (taskResults []*TaskResult, err error) {
	// retrieve the span from the task's context and finish it as soon as this function returns
	if span := opentracing.SpanFromContext(t.Context); span != nil {
		defer span.Finish()
	}

	defer func() {
		// Recover from panic and set err.
		if e := recover(); e != nil {
			switch e := e.(type) {
			default:
				err = ErrTaskPanicked
			case error:
				err = e
			case string:
				err = errors.New(e)
			}

			// mark the span as failed and dump the error and stack trace to the span
			if span := opentracing.SpanFromContext(t.Context); span != nil {
				opentracing_ext.Error.Set(span, true)
				span.LogFields(
					opentracing_log.Error(err),
					opentracing_log.Object("stack", string(debug.Stack())),
				)
			}

			// Print stack trace
			slog.Error("task panicked", "stack", string(debug.Stack()))
		}
	}()

	args := t.Args

	if t.UseContext {
		ctxValue := reflect.ValueOf(t.Context)
		args = append([]reflect.Value{ctxValue}, args...)
	}

	// Invoke the task
	results := t.TaskFunc.Call(args)

	// Task must return at least a value
	if len(results) == 0 {
		return nil, ErrTaskReturnsNoValue
	}

	// Last returned value
	lastResult := results[len(results)-1]

	// IsNil panics for non-nilable kinds (e.g. a struct or int), so only
	// test nilability for kinds that can hold nil; anything else is treated
	// as a non-nil return value
	lastIsNil := false
	switch lastResult.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		lastIsNil = lastResult.IsNil()
	}

	// If the last returned value is not nil, it has to be of error type, if that
	// is not the case, return error message, otherwise propagate the task error
	// to the caller
	if !lastIsNil {
		// If the result implements Retriable interface, return instance of Retriable
		retriableErrorInterface := reflect.TypeOf((*Retriable)(nil)).Elem()
		if lastResult.Type().Implements(retriableErrorInterface) {
			// only the concrete ErrRetryTaskLater carries machinery's retry
			// semantics; other Retriable implementations fall through to the
			// standard error handling below instead of panicking on the
			// unchecked assertion
			if retriableErr, ok := lastResult.Interface().(ErrRetryTaskLater); ok {
				return nil, retriableErr
			}
		}

		// Otherwise, check that the result implements the standard error interface,
		// if not, return ErrLastReturnValueMustBeError error
		errorInterface := reflect.TypeOf((*error)(nil)).Elem()
		if !lastResult.Type().Implements(errorInterface) {
			return nil, ErrLastReturnValueMustBeError
		}

		// Return the standard error
		return nil, lastResult.Interface().(error)
	}

	// Convert reflect values to task results
	taskResults = make([]*TaskResult, len(results)-1)
	for i := 0; i < len(results)-1; i++ {
		val := results[i].Interface()
		valType := reflect.TypeOf(val)
		if valType == nil {
			// an untyped nil cannot be serialized into a TaskResult
			return nil, fmt.Errorf("task returned an untyped nil value at result index %d", i)
		}
		taskResults[i] = &TaskResult{
			Type:  valType.String(),
			Value: val,
		}
	}

	return taskResults, err
}

// ReflectArgs converts []TaskArg to []reflect.Value
func (t *Task) ReflectArgs(args []Arg) error {
	argValues := make([]reflect.Value, len(args))

	for i, arg := range args {
		argValue, err := ReflectValue(arg.Type, arg.Value)
		if err != nil {
			return err
		}
		argValues[i] = argValue
	}

	t.Args = argValues
	return nil
}

// ErrRetryTaskLater instructs the worker to retry the task at a specific time.
type ErrRetryTaskLater struct {
	name, msg string
	retryIn   time.Duration
}

// RetryIn returns time.Duration from now when task should be retried
func (e ErrRetryTaskLater) RetryIn() time.Duration {
	return e.retryIn
}

// Error implements the error interface
func (e ErrRetryTaskLater) Error() string {
	return fmt.Sprintf("Task error: %s Will retry in: %s", e.msg, e.retryIn)
}

// NewErrRetryTaskLater returns new ErrRetryTaskLater instance
func NewErrRetryTaskLater(msg string, retryIn time.Duration) ErrRetryTaskLater {
	return ErrRetryTaskLater{msg: msg, retryIn: retryIn}
}

// Retriable is interface that retriable errors should implement
type Retriable interface {
	RetryIn() time.Duration
}

// TaskResult represents an actual return value of a processed task
type TaskResult struct {
	Type  string
	Value any
}

// ReflectTaskResults converts task results into reflect values.
func ReflectTaskResults(taskResults []*TaskResult) ([]reflect.Value, error) {
	resultValues := make([]reflect.Value, len(taskResults))
	for i, taskResult := range taskResults {
		resultValue, err := ReflectValue(taskResult.Type, taskResult.Value)
		if err != nil {
			return nil, err
		}
		resultValues[i] = resultValue
	}
	return resultValues, nil
}

// HumanReadableResults renders task results as a human readable string.
func HumanReadableResults(results []reflect.Value) string {
	if len(results) == 1 {
		return fmt.Sprintf("%v", results[0].Interface())
	}

	readableResults := make([]string, len(results))
	for i := 0; i < len(results); i++ {
		readableResults[i] = fmt.Sprintf("%v", results[i].Interface())
	}

	return fmt.Sprintf("[%s]", strings.Join(readableResults, ", "))
}

// Arg represents a single argument passed to invocation fo a task
type Arg struct {
	Name  string
	Type  string
	Value any
}

// Headers represents the headers which should be used to direct the task
type Headers map[string]any

// Set on Headers implements opentracing.TextMapWriter for trace propagation
func (h Headers) Set(key, val string) {
	h[key] = val
}

// ForeachKey on Headers implements opentracing.TextMapReader for trace propagation.
// It is essentially the same as the opentracing.TextMapReader implementation except
// for the added casting from any to string.
func (h Headers) ForeachKey(handler func(key, val string) error) error {
	for k, v := range h {
		// Skip any non string values
		stringValue, ok := v.(string)
		if !ok {
			continue
		}

		if err := handler(k, stringValue); err != nil {
			return err
		}
	}

	return nil
}

// Signature represents a single task invocation
type Signature struct {
	UUID           string
	Name           string
	RoutingKey     string
	ETA            *time.Time
	GroupUUID      string
	GroupTaskCount int
	Args           []Arg
	Headers        Headers
	Immutable      bool
	RetryCount     int
	RetryTimeout   int
	OnSuccess      []*Signature
	OnError        []*Signature
	ChordCallback  *Signature
	// IgnoreWhenTaskNotRegistered auto removes the request when there is no handeler available
	// When this is true a task with no handler will be ignored and not placed back in the queue
	IgnoreWhenTaskNotRegistered bool
}

// NewSignature creates a new task signature
func NewSignature(name string, args []Arg) (*Signature, error) {
	signatureID := uuid.New().String()
	return &Signature{
		UUID: fmt.Sprintf("task_%v", signatureID),
		Name: name,
		Args: args,
	}, nil
}

func CopySignatures(signatures ...*Signature) []*Signature {
	var sigs = make([]*Signature, len(signatures))
	for index, signature := range signatures {
		sigs[index] = CopySignature(signature)
	}
	return sigs
}

// CopySignature deep-copies a signature via a JSON round-trip. Decoding with
// UseNumber keeps every number exact (no float64 conversion), so the copy is
// lossless for int64/uint64 arguments and immune to the reflection pitfalls
// (cycles, unexported fields) of the previous reflect-based deep copy.
func CopySignature(signature *Signature) *Signature {
	if signature == nil {
		return nil
	}
	data, err := json.Marshal(signature)
	if err != nil {
		// only reachable for arg values that cannot be marshaled; such a
		// signature would fail to publish anyway
		slog.Error("failed to copy task signature", "task", signature.UUID, "error", err)
		return nil
	}
	var sig *Signature
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&sig); err != nil {
		slog.Error("failed to copy task signature", "task", signature.UUID, "error", err)
		return nil
	}
	return sig
}

const (
	// StatePending - initial state of a task
	StatePending = "PENDING"
	// StateReceived - when task is received by a worker
	StateReceived = "RECEIVED"
	// StateStarted - when the worker starts processing the task
	StateStarted = "STARTED"
	// StateRetry - when failed task has been scheduled for retry
	StateRetry = "RETRY"
	// StateSuccess - when the task is processed successfully
	StateSuccess = "SUCCESS"
	// StateFailure - when processing of the task fails
	StateFailure = "FAILURE"
)

// TaskState represents a state of a task
type TaskState struct {
	TaskUUID  string
	TaskName  string
	State     string
	Results   []*TaskResult
	Error     string
	CreatedAt time.Time
}

// GroupMeta stores useful metadata about tasks within the same group
// E.g. UUIDs of all tasks which are used in order to check if all tasks
// completed successfully or not and thus whether to trigger chord callback
type GroupMeta struct {
	GroupUUID      string
	TaskUUIDs      []string
	ChordTriggered bool
	CreatedAt      time.Time
}

// NewPendingTaskState returns a task state set to PENDING.
func NewPendingTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		TaskName:  signature.Name,
		State:     StatePending,
		CreatedAt: time.Now().UTC(),
	}
}

// NewReceivedTaskState returns a task state set to RECEIVED.
func NewReceivedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateReceived,
	}
}

// NewStartedTaskState returns a task state set to STARTED.
func NewStartedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateStarted,
	}
}

// NewSuccessTaskState returns a task state set to SUCCESS.
func NewSuccessTaskState(signature *Signature, results []*TaskResult) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateSuccess,
		Results:  results,
	}
}

// NewFailureTaskState returns a task state set to FAILURE.
func NewFailureTaskState(signature *Signature, err string) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateFailure,
		Error:    err,
	}
}

// NewRetryTaskState returns a task state set to RETRY.
func NewRetryTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateRetry,
	}
}

// IsCompleted returns true if state is SUCCESS or FAILURE,
// i.e. the task has finished processing and either succeeded or failed.
func (taskState *TaskState) IsCompleted() bool {
	return taskState.IsSuccess() || taskState.IsFailure()
}

// IsSuccess returns true if state is SUCCESS
func (taskState *TaskState) IsSuccess() bool {
	return taskState.State == StateSuccess
}

// IsFailure returns true if state is FAILURE
func (taskState *TaskState) IsFailure() bool {
	return taskState.State == StateFailure
}

// Chain creates a chain of tasks to be executed one after another
type Chain struct {
	Tasks []*Signature
}

// Group creates a set of tasks to be executed in parallel
type Group struct {
	GroupUUID string
	Tasks     []*Signature
}

// Chord adds an optional callback to the group to be executed
// after all tasks in the group finished
type Chord struct {
	Group    *Group
	Callback *Signature
}

// GetUUIDs returns slice of task UUIDS
func (group *Group) GetUUIDs() []string {
	taskUUIDs := make([]string, len(group.Tasks))
	for i, signature := range group.Tasks {
		taskUUIDs[i] = signature.UUID
	}
	return taskUUIDs
}

// NewChain creates a new chain of tasks to be processed one by one, passing
// results unless task signatures are set to be immutable.
// Note: the passed signatures are mutated (UUIDs filled in, OnSuccess wired).
func NewChain(signatures ...*Signature) *Chain {
	// Auto generate task UUIDs if needed
	for _, signature := range signatures {
		if signature.UUID == "" {
			signatureID := uuid.New().String()
			signature.UUID = fmt.Sprintf("task_%v", signatureID)
		}
	}

	for i := len(signatures) - 1; i > 0; i-- {
		signatures[i-1].OnSuccess = []*Signature{signatures[i]}
	}

	return &Chain{Tasks: signatures}
}

// NewGroup creates a new group of tasks to be processed in parallel.
// Note: the passed signatures are mutated (UUIDs / group fields filled in).
func NewGroup(signatures ...*Signature) *Group {
	// Generate a group UUID
	groupUUID := uuid.New().String()
	groupID := fmt.Sprintf("group_%v", groupUUID)

	// Auto generate task UUIDs if needed, group tasks by common group UUID
	for _, signature := range signatures {
		if signature.UUID == "" {
			signatureID := uuid.New().String()
			signature.UUID = fmt.Sprintf("task_%v", signatureID)
		}
		signature.GroupUUID = groupID
		signature.GroupTaskCount = len(signatures)
	}

	return &Group{
		GroupUUID: groupID,
		Tasks:     signatures,
	}
}

// NewChord creates a new chord (a group of tasks with a single callback
// to be executed after all tasks in the group has completed)
func NewChord(group *Group, callback *Signature) (*Chord, error) {
	if group == nil {
		return nil, errors.New("chord group must not be nil")
	}
	if callback == nil {
		return nil, errors.New("chord callback must not be nil")
	}

	if callback.UUID == "" {
		// Generate a UUID for the chord callback
		callbackUUID := uuid.New().String()
		callback.UUID = fmt.Sprintf("chord_%v", callbackUUID)
	}

	// Add a chord callback to all tasks
	for _, signature := range group.Tasks {
		signature.ChordCallback = callback
	}

	return &Chord{Group: group, Callback: callback}, nil
}

var (
	// ErrTaskMustBeFunc ...
	ErrTaskMustBeFunc = errors.New("Task must be a func type")
	// ErrTaskReturnsNoValue ...
	ErrTaskReturnsNoValue = errors.New("Task must return at least a single value")
	// ErrLastReturnValueMustBeError ..
	ErrLastReturnValueMustBeError = errors.New("Last return value of a task must be error")
)

// ValidateTask validates task function using reflection and makes sure
// it has a proper signature. Functions used as tasks must return at least a
// single value and the last return type must be error
func ValidateTask(task any) error {
	v := reflect.ValueOf(task)
	t := v.Type()

	// Task must be a function
	if t.Kind() != reflect.Func {
		return ErrTaskMustBeFunc
	}

	// Task must return at least a single value
	if t.NumOut() < 1 {
		return ErrTaskReturnsNoValue
	}

	// Last return value must be error
	lastReturnType := t.Out(t.NumOut() - 1)
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !lastReturnType.Implements(errorInterface) {
		return ErrLastReturnValueMustBeError
	}

	return nil
}

var (
	typesMap = map[string]reflect.Type{
		// base types
		"bool":    reflect.TypeOf(true),
		"int":     reflect.TypeOf(int(1)),
		"int8":    reflect.TypeOf(int8(1)),
		"int16":   reflect.TypeOf(int16(1)),
		"int32":   reflect.TypeOf(int32(1)),
		"int64":   reflect.TypeOf(int64(1)),
		"uint":    reflect.TypeOf(uint(1)),
		"uint8":   reflect.TypeOf(uint8(1)),
		"uint16":  reflect.TypeOf(uint16(1)),
		"uint32":  reflect.TypeOf(uint32(1)),
		"uint64":  reflect.TypeOf(uint64(1)),
		"float32": reflect.TypeOf(float32(0.5)),
		"float64": reflect.TypeOf(float64(0.5)),
		"string":  reflect.TypeOf(string("")),
		// slices
		"[]bool":    reflect.TypeOf(make([]bool, 0)),
		"[]int":     reflect.TypeOf(make([]int, 0)),
		"[]int8":    reflect.TypeOf(make([]int8, 0)),
		"[]int16":   reflect.TypeOf(make([]int16, 0)),
		"[]int32":   reflect.TypeOf(make([]int32, 0)),
		"[]int64":   reflect.TypeOf(make([]int64, 0)),
		"[]uint":    reflect.TypeOf(make([]uint, 0)),
		"[]uint8":   reflect.TypeOf(make([]uint8, 0)),
		"[]uint16":  reflect.TypeOf(make([]uint16, 0)),
		"[]uint32":  reflect.TypeOf(make([]uint32, 0)),
		"[]uint64":  reflect.TypeOf(make([]uint64, 0)),
		"[]float32": reflect.TypeOf(make([]float32, 0)),
		"[]float64": reflect.TypeOf(make([]float64, 0)),
		"[]byte":    reflect.TypeOf(make([]byte, 0)),
		"[]string":  reflect.TypeOf([]string{""}),
	}

	ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()

	typeConversionError = func(argValue any, argTypeStr string) error {
		return fmt.Errorf("%v is not %v", argValue, argTypeStr)
	}
)

// ErrUnsupportedType is returned when a task argument has an unsupported type.
type ErrUnsupportedType struct {
	valueType string
}

// NewErrUnsupportedType returns new ErrUnsupportedType
func NewErrUnsupportedType(valueType string) ErrUnsupportedType {
	return ErrUnsupportedType{valueType}
}

// Error method so we implement the error interface
func (e ErrUnsupportedType) Error() string {
	return fmt.Sprintf("%v is not one of supported types", e.valueType)
}

// ReflectValue converts any to reflect.Value based on string type
func ReflectValue(valueType string, value any) (reflect.Value, error) {
	if strings.HasPrefix(valueType, "[]") {
		return reflectValues(valueType, value)
	}

	return reflectValue(valueType, value)
}

// reflectValue converts any to reflect.Value based on string type
// representing a base type (not a slice)
func reflectValue(valueType string, value any) (reflect.Value, error) {
	theType, ok := typesMap[valueType]
	if !ok {
		return reflect.Value{}, NewErrUnsupportedType(valueType)
	}
	theValue := reflect.New(theType)

	// Booleans
	if theType.String() == "bool" {
		boolValue, err := getBoolValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetBool(boolValue)
		return theValue.Elem(), nil
	}

	// Integers
	if strings.HasPrefix(theType.String(), "int") {
		intValue, err := getIntValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		// reject values that don't fit the target type (e.g. 300 into int8)
		// instead of silently truncating them
		if theValue.Elem().OverflowInt(intValue) {
			return reflect.Value{}, typeConversionError(value, theType.String())
		}

		theValue.Elem().SetInt(intValue)
		return theValue.Elem(), nil
	}

	// Unsigned integers
	if strings.HasPrefix(theType.String(), "uint") {
		uintValue, err := getUintValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		if theValue.Elem().OverflowUint(uintValue) {
			return reflect.Value{}, typeConversionError(value, theType.String())
		}

		theValue.Elem().SetUint(uintValue)
		return theValue.Elem(), nil
	}

	// Floating point numbers
	if strings.HasPrefix(theType.String(), "float") {
		floatValue, err := getFloatValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		if theValue.Elem().OverflowFloat(floatValue) {
			return reflect.Value{}, typeConversionError(value, theType.String())
		}

		theValue.Elem().SetFloat(floatValue)
		return theValue.Elem(), nil
	}

	// Strings
	if theType.String() == "string" {
		stringValue, err := getStringValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetString(stringValue)
		return theValue.Elem(), nil
	}

	return reflect.Value{}, NewErrUnsupportedType(valueType)
}

// reflectValues converts any to reflect.Value based on string type
// representing a slice of values
func reflectValues(valueType string, value any) (reflect.Value, error) {
	theType, ok := typesMap[valueType]
	if !ok {
		return reflect.Value{}, NewErrUnsupportedType(valueType)
	}

	// For NULL we return an empty slice
	if value == nil {
		return reflect.MakeSlice(theType, 0, 0), nil
	}

	// The conversions below call Len/Index, which panic for other kinds;
	// string stays allowed because the []byte/[]uint8 branch decodes
	// base64 from a string
	switch reflect.ValueOf(value).Kind() {
	case reflect.Slice, reflect.Array, reflect.String:
	default:
		return reflect.Value{}, typeConversionError(value, theType.String())
	}

	var theValue reflect.Value

	// Booleans
	if theType.String() == "[]bool" {
		bools := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, bools.Len(), bools.Len())
		for i := 0; i < bools.Len(); i++ {
			boolValue, err := getBoolValue(strings.Split(theType.String(), "[]")[1], bools.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetBool(boolValue)
		}

		return theValue, nil
	}

	// Integers
	if strings.HasPrefix(theType.String(), "[]int") {
		ints := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, ints.Len(), ints.Len())
		for i := 0; i < ints.Len(); i++ {
			intValue, err := getIntValue(strings.Split(theType.String(), "[]")[1], ints.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetInt(intValue)
		}

		return theValue, nil
	}

	// Unsigned integers
	if strings.HasPrefix(theType.String(), "[]uint") || theType.String() == "[]byte" {

		// Decode the base64 string if the value type is []uint8 or it's alias []byte
		// See: https://golang.org/pkg/encoding/json/#Marshal
		// > Array and slice values encode as JSON arrays, except that []byte encodes as a base64-encoded string
		if reflect.TypeOf(value).String() == "string" {
			output, err := base64.StdEncoding.DecodeString(value.(string))
			if err != nil {
				return reflect.Value{}, err
			}
			value = output
		}

		uints := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, uints.Len(), uints.Len())
		for i := 0; i < uints.Len(); i++ {
			uintValue, err := getUintValue(strings.Split(theType.String(), "[]")[1], uints.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetUint(uintValue)
		}

		return theValue, nil
	}

	// Floating point numbers
	if strings.HasPrefix(theType.String(), "[]float") {
		floats := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, floats.Len(), floats.Len())
		for i := 0; i < floats.Len(); i++ {
			floatValue, err := getFloatValue(strings.Split(theType.String(), "[]")[1], floats.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetFloat(floatValue)
		}

		return theValue, nil
	}

	// Strings
	if theType.String() == "[]string" {
		strs := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, strs.Len(), strs.Len())
		for i := 0; i < strs.Len(); i++ {
			strValue, err := getStringValue(strings.Split(theType.String(), "[]")[1], strs.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetString(strValue)
		}

		return theValue, nil
	}

	return reflect.Value{}, NewErrUnsupportedType(valueType)
}

func getBoolValue(theType string, value any) (bool, error) {
	b, ok := value.(bool)
	if !ok {
		return false, typeConversionError(value, typesMap[theType].String())
	}

	return b, nil
}

func getIntValue(theType string, value any) (int64, error) {
	// We use https://golang.org/pkg/encoding/json/#Decoder.UseNumber when unmarshaling signatures.
	// This is because JSON only supports 64-bit floating point numbers and we could lose precision
	// when converting from float64 to signed integer
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		return n.Int64()
	}

	n, ok := value.(int64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return n, nil
}

func getUintValue(theType string, value any) (uint64, error) {
	// Losing precision only happens in receiving a JSON number from a language like js,
	// and receiving a large uint number from golang or python could cause json.Number.Int64 be turned into a panic.
	// So we use strconv.ParseUint to correctly parse a uint value.
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		uintVal, err := strconv.ParseUint(string(n), 10, 64)
		if err != nil {
			return 0, err
		}

		return uintVal, nil
	}

	var n uint64
	switch value := value.(type) {
	case uint64:
		n = value
	case uint8:
		n = uint64(value)
	default:
		return 0, typeConversionError(value, typesMap[theType].String())
	}
	return n, nil
}

func getFloatValue(theType string, value any) (float64, error) {
	// We use https://golang.org/pkg/encoding/json/#Decoder.UseNumber when unmarshaling signatures.
	// This is because JSON only supports 64-bit floating point numbers and we could lose precision
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		return n.Float64()
	}

	f, ok := value.(float64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return f, nil
}

func getStringValue(theType string, value any) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", typeConversionError(value, typesMap[theType].String())
	}

	return s, nil
}

// IsContextType checks to see if the type is a context.Context
func IsContextType(t reflect.Type) bool {
	return t == ctxType
}
