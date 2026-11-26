package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions
	InitGroup(ctx context.Context, groupUUID string, taskUUIDs []string) error
	GroupCompleted(ctx context.Context, groupUUID string, groupTaskCount int) (bool, error)
	GroupTaskStates(ctx context.Context, groupUUID string, groupTaskCount int) ([]*TaskState, error)
	TriggerChord(ctx context.Context, groupUUID string) (bool, error)

	// Setting / getting task state
	SetStatePending(ctx context.Context, signature *Signature) error
	SetStateReceived(ctx context.Context, signature *Signature) error
	SetStateStarted(ctx context.Context, signature *Signature) error
	SetStateRetry(ctx context.Context, signature *Signature) error
	SetStateSuccess(ctx context.Context, signature *Signature, results []*TaskResult) error
	SetStateFailure(ctx context.Context, signature *Signature, err string) error
	GetState(ctx context.Context, taskUUID string) (*TaskState, error)

	// Purging stored task states and group meta data
	PurgeState(ctx context.Context, taskUUID string) error
	PurgeGroupMeta(ctx context.Context, groupUUID string) error
}

// BaseBackend represents a base backend structure
type BaseBackend struct {
	cnf *Config
}

// NewBaseBackend creates new BaseBackend instance
func NewBaseBackend(cnf *Config) BaseBackend {
	return BaseBackend{cnf: cnf}
}

// GetConfig returns config
func (b *BaseBackend) GetConfig() *Config {
	return b.cnf
}

// RedisBackend represents a Redis result backend
type RedisBackend struct {
	BaseBackend
	rclient redis.UniversalClient
	redsync *redsync.Redsync
}

// NewRedisBackendWithClient creates RedisBackend instance with an externally owned
// redis.UniversalClient. The caller owns the client lifecycle.
func NewRedisBackendWithClient(cnf *Config, client redis.UniversalClient) Backend {
	return &RedisBackend{
		BaseBackend: NewBaseBackend(cnf),
		rclient:     client,
		redsync:     redsync.New(redsyncgoredis.NewPool(client)),
	}
}

// InitGroup creates and saves a group meta data object
func (b *RedisBackend) InitGroup(ctx context.Context, groupUUID string, taskUUIDs []string) error {
	groupMeta := &GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	err = b.rclient.Set(ctx, groupUUID, encoded, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *RedisBackend) GroupCompleted(ctx context.Context, groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(ctx, groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(ctx, groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *RedisBackend) GroupTaskStates(ctx context.Context, groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	groupMeta, err := b.getGroupMeta(ctx, groupUUID)
	if err != nil {
		return []*TaskState{}, err
	}

	return b.getStates(ctx, groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never triggered multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *RedisBackend) TriggerChord(ctx context.Context, groupUUID string) (bool, error) {
	// per-group key: a single global name serializes every chord trigger of
	// every group (and every instance sharing the same Redis) on one lock
	m := b.redsync.NewMutex(fmt.Sprintf("TriggerChordMutex-%s", groupUUID))
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer func() {
		if _, err := m.Unlock(); err != nil {
			slog.Warn("failed to unlock chord mutex", "group", groupUUID, "error", err)
		}
	}()

	groupMeta, err := b.getGroupMeta(ctx, groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	expiration := b.getExpiration()
	err = b.rclient.Set(ctx, groupUUID, encoded, expiration).Err()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *RedisBackend) mergeNewTaskState(ctx context.Context, newState *TaskState) error {
	state, err := b.GetState(ctx, newState.TaskUUID)
	if err != nil {
		// no prior state stored yet — nothing to merge, that's expected for
		// a fresh task; any other error must not silently drop
		// CreatedAt/TaskName from the state we are about to persist
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return err
	}
	newState.CreatedAt = state.CreatedAt
	newState.TaskName = state.TaskName
	return nil
}

// SetStatePending updates task state to PENDING
func (b *RedisBackend) SetStatePending(ctx context.Context, signature *Signature) error {
	taskState := NewPendingTaskState(signature)
	return b.updateState(ctx, taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *RedisBackend) SetStateReceived(ctx context.Context, signature *Signature) error {
	taskState := NewReceivedTaskState(signature)
	if err := b.mergeNewTaskState(ctx, taskState); err != nil {
		return err
	}
	return b.updateState(ctx, taskState)
}

// SetStateStarted updates task state to STARTED
func (b *RedisBackend) SetStateStarted(ctx context.Context, signature *Signature) error {
	taskState := NewStartedTaskState(signature)
	if err := b.mergeNewTaskState(ctx, taskState); err != nil {
		return err
	}
	return b.updateState(ctx, taskState)
}

// SetStateRetry updates task state to RETRY
func (b *RedisBackend) SetStateRetry(ctx context.Context, signature *Signature) error {
	taskState := NewRetryTaskState(signature)
	if err := b.mergeNewTaskState(ctx, taskState); err != nil {
		return err
	}
	return b.updateState(ctx, taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *RedisBackend) SetStateSuccess(ctx context.Context, signature *Signature, results []*TaskResult) error {
	taskState := NewSuccessTaskState(signature, results)
	if err := b.mergeNewTaskState(ctx, taskState); err != nil {
		return err
	}
	return b.updateState(ctx, taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *RedisBackend) SetStateFailure(ctx context.Context, signature *Signature, err string) error {
	taskState := NewFailureTaskState(signature, err)
	if err := b.mergeNewTaskState(ctx, taskState); err != nil {
		return err
	}
	return b.updateState(ctx, taskState)
}

// GetState returns the latest task state
func (b *RedisBackend) GetState(ctx context.Context, taskUUID string) (*TaskState, error) {

	item, err := b.rclient.Get(ctx, taskUUID).Bytes()
	if err != nil {
		return nil, err
	}
	state := new(TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *RedisBackend) PurgeState(ctx context.Context, taskUUID string) error {
	err := b.rclient.Del(ctx, taskUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *RedisBackend) PurgeGroupMeta(ctx context.Context, groupUUID string) error {
	err := b.rclient.Del(ctx, groupUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *RedisBackend) getGroupMeta(ctx context.Context, groupUUID string) (*GroupMeta, error) {
	item, err := b.rclient.Get(ctx, groupUUID).Bytes()
	if err != nil {
		return nil, err
	}

	groupMeta := new(GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *RedisBackend) getStates(ctx context.Context, taskUUIDs ...string) ([]*TaskState, error) {
	taskStates := make([]*TaskState, len(taskUUIDs))
	// to avoid CROSSSLOT error, use pipeline
	cmders, err := b.rclient.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		for _, uuid := range taskUUIDs {
			pipeliner.Get(ctx, uuid)
		}
		return nil
	})
	if err != nil {
		return taskStates, err
	}
	for i, cmder := range cmders {
		stateBytes, err1 := cmder.(*redis.StringCmd).Bytes()
		if err1 != nil {
			return taskStates, err1
		}
		taskState := new(TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err1 = decoder.Decode(taskState); err1 != nil {
			slog.Error("failed to decode task state", "error", err1)
			return taskStates, err1
		}
		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *RedisBackend) updateState(ctx context.Context, taskState *TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	_, err = b.rclient.Set(ctx, taskState.TaskUUID, encoded, expiration).Result()
	if err != nil {
		return err
	}

	return nil
}

// getExpiration returns expiration for a stored task state
func (b *RedisBackend) getExpiration() time.Duration {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// expire results after 1 hour by default
		expiresIn = DefaultResultsExpireIn
	}

	return time.Duration(expiresIn) * time.Second
}

var (
	// ErrBackendNotConfigured ...
	ErrBackendNotConfigured = errors.New("Result backend not configured")
	// ErrTimeoutReached ...
	ErrTimeoutReached = errors.New("Timeout reached")
)

// AsyncResult represents a task result
type AsyncResult struct {
	Signature *Signature
	taskState *TaskState
	backend   Backend
	// lastErr holds the last non-transient backend error (anything other
	// than redis.Nil) so polling loops can surface it instead of treating
	// a failing backend as "still pending" and polling forever
	lastErr error
}

// ChordAsyncResult represents a result of a chord
type ChordAsyncResult struct {
	groupAsyncResults []*AsyncResult
	chordAsyncResult  *AsyncResult
	backend           Backend
}

// ChainAsyncResult represents a result of a chain of tasks
type ChainAsyncResult struct {
	asyncResults []*AsyncResult
	backend      Backend
}

// NewAsyncResult creates AsyncResult instance
func NewAsyncResult(signature *Signature, backend Backend) *AsyncResult {
	return &AsyncResult{
		Signature: signature,
		taskState: new(TaskState),
		backend:   backend,
	}
}

// NewChordAsyncResult creates ChordAsyncResult instance
func NewChordAsyncResult(groupTasks []*Signature, chordCallback *Signature, backend Backend) *ChordAsyncResult {
	asyncResults := make([]*AsyncResult, len(groupTasks))
	for i, task := range groupTasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}
	return &ChordAsyncResult{
		groupAsyncResults: asyncResults,
		chordAsyncResult:  NewAsyncResult(chordCallback, backend),
		backend:           backend,
	}
}

// NewChainAsyncResult creates ChainAsyncResult instance
func NewChainAsyncResult(tasks []*Signature, backend Backend) *ChainAsyncResult {
	asyncResults := make([]*AsyncResult, len(tasks))
	for i, task := range tasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}
	return &ChainAsyncResult{
		asyncResults: asyncResults,
		backend:      backend,
	}
}

// defaultPollSleep is used when a caller passes a non-positive sleep duration,
// which would otherwise turn the polling loops below into a busy loop
// hammering the backend
const defaultPollSleep = time.Second

// TaskFailedError is returned by Touch/Get when the task finished in the
// FAILURE state. It is a typed error so callers can distinguish "the task
// itself failed" from transport/backend problems via errors.As.
type TaskFailedError struct {
	TaskUUID string
	Message  string
}

// Error implements the error interface
func (e *TaskFailedError) Error() string {
	return fmt.Sprintf("task %s failed: %s", e.TaskUUID, e.Message)
}

// Touch the state and don't wait
func (asyncResult *AsyncResult) Touch(ctx context.Context) ([]reflect.Value, error) {
	if asyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	asyncResult.GetState(ctx)

	// surface backend failures (connection errors, decode errors, ...)
	// instead of reporting "still pending" forever
	if asyncResult.lastErr != nil {
		return nil, asyncResult.lastErr
	}

	if asyncResult.taskState.IsFailure() {
		return nil, &TaskFailedError{
			TaskUUID: asyncResult.Signature.UUID,
			Message:  asyncResult.taskState.Error,
		}
	}

	if asyncResult.taskState.IsSuccess() {
		return ReflectTaskResults(asyncResult.taskState.Results)
	}

	return nil, nil
}

// Get returns task results when they arrive, or ctx.Err() if the context is
// cancelled first (synchronous blocking call)
func (asyncResult *AsyncResult) Get(ctx context.Context, sleepDuration time.Duration) ([]reflect.Value, error) {
	if sleepDuration <= 0 {
		sleepDuration = defaultPollSleep
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		results, err := asyncResult.Touch(ctx)
		if results == nil && err == nil {
			timer := time.NewTimer(sleepDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		} else {
			return results, err
		}
	}
}

// GetWithTimeout returns task results with a timeout (synchronous blocking call)
func (asyncResult *AsyncResult) GetWithTimeout(ctx context.Context, timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	ctx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	results, err := asyncResult.Get(ctx, sleepDuration)
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, ErrTimeoutReached
	}
	return results, err
}

// GetState returns latest task state
func (asyncResult *AsyncResult) GetState(ctx context.Context) *TaskState {
	if asyncResult.backend == nil {
		// guard against a nil backend, matching Touch's ErrBackendNotConfigured
		// behavior (Touch calls this method, so no error return here)
		return asyncResult.taskState
	}

	if asyncResult.taskState.IsCompleted() {
		return asyncResult.taskState
	}

	taskState, err := asyncResult.backend.GetState(ctx, asyncResult.Signature.UUID)
	if err != nil {
		// redis.Nil simply means the state isn't stored yet — keep polling;
		// any other error is remembered and surfaced by Touch
		if !errors.Is(err, redis.Nil) {
			asyncResult.lastErr = err
		}
		return asyncResult.taskState
	}

	asyncResult.lastErr = nil
	asyncResult.taskState = taskState

	return asyncResult.taskState
}

// Get returns results of a chain of tasks once every task has finished
// (synchronous blocking call)
func (chainAsyncResult *ChainAsyncResult) Get(ctx context.Context, sleepDuration time.Duration) ([]reflect.Value, error) {
	if chainAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	for _, asyncResult := range chainAsyncResult.asyncResults {
		results, err = asyncResult.Get(ctx, sleepDuration)
		if err != nil {
			return nil, err
		}
	}

	return results, err
}

// Get returns result of a chord once every group task and the callback have
// finished (synchronous blocking call)
func (chordAsyncResult *ChordAsyncResult) Get(ctx context.Context, sleepDuration time.Duration) ([]reflect.Value, error) {
	if chordAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	for _, asyncResult := range chordAsyncResult.groupAsyncResults {
		if _, err := asyncResult.Get(ctx, sleepDuration); err != nil {
			return nil, err
		}
	}

	return chordAsyncResult.chordAsyncResult.Get(ctx, sleepDuration)
}

// GetWithTimeout returns results of a chain of tasks with timeout (synchronous blocking call)
func (chainAsyncResult *ChainAsyncResult) GetWithTimeout(ctx context.Context, timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	if chainAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	// an empty chain has nothing to wait for
	if len(chainAsyncResult.asyncResults) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	results, err := chainAsyncResult.Get(ctx, sleepDuration)
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, ErrTimeoutReached
	}
	return results, err
}

// GetWithTimeout returns result of a chord with a timeout (synchronous blocking call)
func (chordAsyncResult *ChordAsyncResult) GetWithTimeout(ctx context.Context, timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	if chordAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	ctx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	results, err := chordAsyncResult.Get(ctx, sleepDuration)
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, ErrTimeoutReached
	}
	return results, err
}
