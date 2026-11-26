// Package queue is an asynchronous task queue based on distributed
// message passing, backed by Redis (single instance, Sentinel or cluster).
package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/robfig/cron/v3"
)

// Server is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the server
type Server struct {
	config            *Config
	registeredTasks   *sync.Map
	broker            Broker
	backend           Backend
	lock              Lock
	scheduler         *cron.Cron
	schedulerOnce     sync.Once
	shutdown          atomic.Bool
	prePublishHandler func(*Signature)

	workersMu  sync.Mutex
	workers    []*Worker
	signalOnce sync.Once
}

// slogCronLogger adapts slog to the cron.Logger interface so cron panics are
// recovered and reported instead of taking down the scheduler goroutine.
type slogCronLogger struct{ logger *slog.Logger }

func (l slogCronLogger) Info(msg string, keysAndValues ...interface{}) {
	l.logger.Info(msg, keysAndValues...)
}

func (l slogCronLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	l.logger.Error(msg, append([]interface{}{"error", err}, keysAndValues...)...)
}

// NewServer creates a Server instance. The broker, backend and lock must be
// non-nil (build clients with NewGoRedisClient); the DefaultQueue config
// field is required.
func NewServer(cnf *Config, brokerServer Broker, backendServer Backend, lock Lock) (*Server, error) {
	if cnf == nil {
		return nil, ErrInvalidArgument
	}
	if cnf.DefaultQueue == "" {
		return nil, errors.New("config: DefaultQueue must be set")
	}
	if brokerServer == nil {
		return nil, errors.New("broker must not be nil")
	}
	if backendServer == nil {
		return nil, errors.New("backend must not be nil")
	}
	if lock == nil {
		return nil, errors.New("lock must not be nil")
	}

	cronLogger := slogCronLogger{slog.Default().With("component", "cron")}
	srv := &Server{
		config:          cnf,
		registeredTasks: new(sync.Map),
		broker:          brokerServer,
		backend:         backendServer,
		lock:            lock,
		scheduler: cron.New(
			cron.WithChain(cron.Recover(cronLogger)),
			cron.WithLogger(cronLogger),
		),
	}

	// keep a single source of truth for task registration: the broker asks
	// the server instead of holding its own name snapshot
	brokerServer.SetTaskRegisteredFunc(srv.IsTaskRegistered)

	return srv, nil
}

// startScheduler lazily starts the cron goroutine on the first periodic-task
// registration; a server without periodic tasks never pays for it.
func (server *Server) startScheduler() {
	server.schedulerOnce.Do(func() {
		go server.scheduler.Run()
	})
}

// Shutdown stops the periodic-task scheduler and gracefully quits every
// worker launched by this server. The returned context completes when all
// running cron jobs have finished.
func (server *Server) Shutdown() context.Context {
	server.shutdown.Store(true)

	server.workersMu.Lock()
	workers := append([]*Worker(nil), server.workers...)
	server.workersMu.Unlock()
	for _, w := range workers {
		w.beginGracefulQuit()
	}

	return server.scheduler.Stop()
}

func (server *Server) registerWorker(worker *Worker) {
	server.workersMu.Lock()
	defer server.workersMu.Unlock()
	server.workers = append(server.workers, worker)
}

// NewWorker creates Worker instance
func (server *Server) NewWorker(consumerTag string, concurrency int) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       "",
	}
}

// NewCustomQueueWorker creates Worker instance with Custom Queue
func (server *Server) NewCustomQueueWorker(consumerTag string, concurrency int, queue string) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       queue,
	}
}

// GetBroker returns broker
func (server *Server) GetBroker() Broker {
	return server.broker
}

// GetBackend returns backend
func (server *Server) GetBackend() Backend {
	return server.backend
}

// GetConfig returns config
func (server *Server) GetConfig() *Config {
	return server.config
}

// SetPreTaskHandler Sets pre publish handler
func (server *Server) SetPreTaskHandler(handler func(*Signature)) {
	server.prePublishHandler = handler
}

// RegisterTasks registers all tasks at once
func (server *Server) RegisterTasks(namedTaskFuncs map[string]any) error {
	for _, task := range namedTaskFuncs {
		if err := ValidateTask(task); err != nil {
			return err
		}
	}
	for k, v := range namedTaskFuncs {
		server.registeredTasks.Store(k, v)
	}
	return nil
}

// RegisterTask registers a single task
func (server *Server) RegisterTask(name string, taskFunc any) error {
	if err := ValidateTask(taskFunc); err != nil {
		return err
	}
	server.registeredTasks.Store(name, taskFunc)
	return nil
}

// IsTaskRegistered returns true if the task name is registered with this broker
func (server *Server) IsTaskRegistered(name string) bool {
	_, ok := server.registeredTasks.Load(name)
	return ok
}

// GetRegisteredTask returns registered task by name
func (server *Server) GetRegisteredTask(name string) (any, error) {
	taskFunc, ok := server.registeredTasks.Load(name)
	if !ok {
		return nil, fmt.Errorf("Task not registered error: %s", name)
	}
	return taskFunc, nil
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
func (server *Server) SendTaskWithContext(ctx context.Context, signature *Signature) (*AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendTask", ProducerOption(), MachineryTag)
	defer span.Finish()

	// tag the span with some info about the signature
	signature.Headers = HeadersWithSpan(signature.Headers, span)

	// Auto generate a UUID if not set already
	if signature.UUID == "" {
		taskID := uuid.New().String()
		signature.UUID = fmt.Sprintf("task_%v", taskID)
	}

	// Set initial task state to PENDING
	if err := server.backend.SetStatePending(ctx, signature); err != nil {
		return nil, fmt.Errorf("Set state pending error: %w", err)
	}

	if server.prePublishHandler != nil {
		server.prePublishHandler(signature)
	}

	if err := server.broker.Publish(ctx, signature); err != nil {
		return nil, fmt.Errorf("Publish message error: %w", err)
	}

	return NewAsyncResult(signature, server.backend), nil
}

// SendTask publishes a task to the default queue
func (server *Server) SendTask(signature *Signature) (*AsyncResult, error) {
	return server.SendTaskWithContext(context.Background(), signature)
}

// SendChainWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChainWithContext(ctx context.Context, chain *Chain) (*ChainAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChain", ProducerOption(), MachineryTag, WorkflowChainTag)
	defer span.Finish()

	AnnotateSpanWithChainInfo(span, chain)

	// An empty chain would panic on the Tasks[0] index below
	if len(chain.Tasks) == 0 {
		return nil, errors.New("chain must contain at least one task")
	}

	// Publish the first task with the caller's context so its span and
	// deadline are not dropped before publishing
	if _, err := server.SendTaskWithContext(ctx, chain.Tasks[0]); err != nil {
		return nil, err
	}

	return NewChainAsyncResult(chain.Tasks, server.backend), nil
}

// SendChain triggers a chain of tasks
func (server *Server) SendChain(chain *Chain) (*ChainAsyncResult, error) {
	return server.SendChainWithContext(context.Background(), chain)
}

// SendGroupWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendGroupWithContext(ctx context.Context, group *Group, sendConcurrency int) ([]*AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendGroup", ProducerOption(), MachineryTag, WorkflowGroupTag)
	defer span.Finish()

	AnnotateSpanWithGroupInfo(span, group, sendConcurrency)

	if sendConcurrency < 0 {
		return nil, fmt.Errorf("sendConcurrency must be >= 0, got %d", sendConcurrency)
	}

	asyncResults := make([]*AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	// capacity covers the worst case: every task failing both its pending
	// write and its publish
	errorsChan := make(chan error, len(group.Tasks)*2)

	// Init group; if the metadata cannot be persisted the group would never
	// complete and a chord callback would silently never run
	if err := server.backend.InitGroup(ctx, group.GroupUUID, group.GetUUIDs()); err != nil {
		return nil, fmt.Errorf("Init group error: %w", err)
	}

	// Init the tasks Pending state first; remember the failed ones so they
	// are not published below without a backend state
	failed := make(map[*Signature]struct{})
	for _, signature := range group.Tasks {
		if err := server.backend.SetStatePending(ctx, signature); err != nil {
			failed[signature] = struct{}{}
			errorsChan <- err
			continue
		}
	}

	pool := make(chan struct{}, sendConcurrency)
	go func() {
		for i := 0; i < sendConcurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for i, signature := range group.Tasks {

		if _, isFailed := failed[signature]; isFailed {
			wg.Done()
			continue
		}

		if sendConcurrency > 0 {
			<-pool
		}

		go func(s *Signature, index int) {
			defer wg.Done()

			// Publish task

			err := server.broker.Publish(ctx, s)

			if sendConcurrency > 0 {
				pool <- struct{}{}
			}

			if err != nil {
				errorsChan <- fmt.Errorf("Publish message error: %w", err)
				return
			}

			asyncResults[index] = NewAsyncResult(s, server.backend)
		}(signature, i)
	}

	// buffered so the watcher goroutine never leaks when the error branch
	// returns without receiving
	done := make(chan int, 1)
	go func() {
		wg.Wait()
		done <- 1
	}()

	select {
	case err := <-errorsChan:
		// wait for in-flight publish goroutines before returning the shared slice
		wg.Wait()
		return asyncResults, err
	case <-done:
		return asyncResults, nil
	}
}

// SendGroup triggers a group of parallel tasks
func (server *Server) SendGroup(group *Group, sendConcurrency int) ([]*AsyncResult, error) {
	return server.SendGroupWithContext(context.Background(), group, sendConcurrency)
}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChordWithContext(ctx context.Context, chord *Chord, sendConcurrency int) (*ChordAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChord", ProducerOption(), MachineryTag, WorkflowChordTag)
	defer span.Finish()

	AnnotateSpanWithChordInfo(span, chord, sendConcurrency)

	_, err := server.SendGroupWithContext(ctx, chord.Group, sendConcurrency)
	if err != nil {
		return nil, err
	}

	return NewChordAsyncResult(
		chord.Group.Tasks,
		chord.Callback,
		server.backend,
	), nil
}

// SendChord triggers a group of parallel tasks with a callback
func (server *Server) SendChord(chord *Chord, sendConcurrency int) (*ChordAsyncResult, error) {
	return server.SendChordWithContext(context.Background(), chord, sendConcurrency)
}

// lockName builds the distributed-lock key for a periodic workflow. AppName
// namespaces the keys across deployments; it defaults to the binary name.
func (server *Server) lockName(name, spec string) string {
	app := server.config.AppName
	if app == "" {
		app = filepath.Base(os.Args[0])
	}
	return LockKeyPrefix + app + "|" + name + "|" + spec
}

// validatePeriodicSignatures rejects nil signatures up front so the cron job
// never dereferences one inside the scheduler goroutine.
func validatePeriodicSignatures(signatures []*Signature) error {
	for i, s := range signatures {
		if s == nil {
			return fmt.Errorf("signature at index %d must not be nil", i)
		}
	}
	return nil
}

// RegisterPeriodicTask register a periodic task which will be triggered periodically
func (server *Server) RegisterPeriodicTask(spec, name string, signature *Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	if signature == nil {
		return errors.New("periodic task requires a signature")
	}
	if server.shutdown.Load() {
		return errors.New("server is shutting down")
	}

	f := func() {
		// Hold the lock until just before the next scheduled fire time so,
		// across deployments, only one instance executes this slot. The lock
		// auto-expires at the deadline (crashed holders don't deadlock).
		ttl := time.Until(schedule.Next(time.Now()))
		if err := server.lock.LockWithRetries(context.Background(), server.lockName(name, spec), ttl); err != nil {
			return
		}

		//send task
		_, err = server.SendTask(CopySignature(signature))
		if err != nil {
			slog.Error("periodic task failed", "task", name, "error", err)
		}
	}

	server.startScheduler()
	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicChain register a periodic chain which will be triggered periodically
func (server *Server) RegisterPeriodicChain(spec, name string, signatures ...*Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	if len(signatures) == 0 {
		return errors.New("periodic chain requires at least one task signature")
	}
	if err := validatePeriodicSignatures(signatures); err != nil {
		return err
	}
	if server.shutdown.Load() {
		return errors.New("server is shutting down")
	}

	f := func() {
		// new chain
		chain := NewChain(CopySignatures(signatures...)...)

		//get lock
		ttl := time.Until(schedule.Next(time.Now()))
		if err := server.lock.LockWithRetries(context.Background(), server.lockName(name, spec), ttl); err != nil {
			return
		}

		//send task
		_, err = server.SendChain(chain)
		if err != nil {
			slog.Error("periodic task failed", "task", name, "error", err)
		}
	}

	server.startScheduler()
	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicGroup register a periodic group which will be triggered periodically
func (server *Server) RegisterPeriodicGroup(spec, name string, sendConcurrency int, signatures ...*Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	if len(signatures) == 0 {
		return errors.New("periodic group requires at least one task signature")
	}
	if err := validatePeriodicSignatures(signatures); err != nil {
		return err
	}
	if server.shutdown.Load() {
		return errors.New("server is shutting down")
	}

	f := func() {
		// new group
		group := NewGroup(CopySignatures(signatures...)...)

		//get lock
		ttl := time.Until(schedule.Next(time.Now()))
		if err := server.lock.LockWithRetries(context.Background(), server.lockName(name, spec), ttl); err != nil {
			return
		}

		//send task
		_, err = server.SendGroup(group, sendConcurrency)
		if err != nil {
			slog.Error("periodic task failed", "task", name, "error", err)
		}
	}

	server.startScheduler()
	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicChord register a periodic chord which will be triggered periodically
func (server *Server) RegisterPeriodicChord(spec, name string, sendConcurrency int, callback *Signature, signatures ...*Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	if callback == nil {
		return errors.New("periodic chord requires a callback signature")
	}
	if len(signatures) == 0 {
		return errors.New("periodic chord requires at least one task signature")
	}
	if err := validatePeriodicSignatures(signatures); err != nil {
		return err
	}
	if server.shutdown.Load() {
		return errors.New("server is shutting down")
	}

	f := func() {
		// new chord
		group := NewGroup(CopySignatures(signatures...)...)
		chord, err := NewChord(group, CopySignature(callback))
		if err != nil {
			slog.Error("failed to build periodic chord", "task", name, "error", err)
			return
		}

		//get lock
		ttl := time.Until(schedule.Next(time.Now()))
		if err := server.lock.LockWithRetries(context.Background(), server.lockName(name, spec), ttl); err != nil {
			return
		}

		//send task
		_, err = server.SendChord(chord, sendConcurrency)
		if err != nil {
			slog.Error("periodic task failed", "task", name, "error", err)
		}
	}

	server.startScheduler()
	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// Worker represents a single worker process
type Worker struct {
	server          *Server
	ConsumerTag     string
	Concurrency     int
	Queue           string
	errorHandler    func(err error)
	preTaskHandler  func(*Signature)
	postTaskHandler func(*Signature)

	// set up by LaunchAsync; read by the server-level signal handler
	errorsChan    chan<- error
	quitInitiated atomic.Bool
	quitDone      chan struct{}
}

var (
	// ErrWorkerQuitGracefully is return when worker quit gracefully
	ErrWorkerQuitGracefully = errors.New("Worker quit gracefully")
	// ErrWorkerQuitGracefully is return when worker quit abruptly
	ErrWorkerQuitAbruptly = errors.New("Worker quit abruptly")
)

// Launch starts a new worker process. The worker subscribes
// to the default queue and processes incoming registered tasks.
// It blocks until the worker stops and returns the reason.
func (worker *Worker) Launch() error {
	errorsChan := make(chan error, 2)

	worker.LaunchAsync(errorsChan)

	return <-errorsChan
}

// LaunchAsync is a non blocking version of Launch
func (worker *Worker) LaunchAsync(errorsChan chan<- error) {
	cnf := worker.server.GetConfig()
	broker := worker.server.GetBroker()

	// Log some useful information about worker configuration
	queue := cnf.DefaultQueue
	if worker.Queue != "" {
		queue = worker.Queue
	}
	slog.Info("launching worker",
		"queue", queue,
		"concurrency", worker.Concurrency,
	)

	worker.errorsChan = errorsChan
	worker.quitDone = make(chan struct{})
	worker.server.registerWorker(worker)

	if !cnf.NoUnixSignals {
		// one signal handler per server, not per worker: all workers of this
		// server quit together on the first signal
		worker.server.signalOnce.Do(worker.server.installSignalHandler)
	}

	// quitDone is closed after Quit() returns; quitInitiated flips before the
	// graceful-quit goroutine is spawned. A channel is used instead of a
	// WaitGroup because WaitGroup.Add concurrent with Wait is undefined.

	// Goroutine to start broker consumption and handle retries when broker connection dies
	go func() {
		for {
			retry, err := broker.StartConsuming(worker.ConsumerTag, worker.Concurrency, worker)

			if retry {
				if worker.errorHandler != nil {
					worker.errorHandler(err)
				} else {
					slog.Warn("broker failed", "error", err)
				}
			} else {
				// Wait for the graceful quit (if one was initiated) to finish
				// before reporting the final error
				if worker.quitInitiated.Load() {
					<-worker.quitDone
				}
				errorsChan <- err // stop the goroutine
				return
			}
		}
	}()
}

// beginGracefulQuit starts the graceful shutdown of this worker at most once.
func (worker *Worker) beginGracefulQuit() {
	if !worker.quitInitiated.CompareAndSwap(false, true) {
		return
	}
	slog.Warn("waiting for running tasks to finish before shutting down")
	go func() {
		worker.Quit()
		close(worker.quitDone)
		// best-effort: the caller may not be reading errorsChan, blocking
		// here would stall shutdown
		select {
		case worker.errorsChan <- ErrWorkerQuitGracefully:
		default:
		}
	}()
}

// handleAbruptQuit feeds the abrupt-quit error to the launcher, best-effort.
func (worker *Worker) handleAbruptQuit() {
	select {
	case worker.errorsChan <- ErrWorkerQuitAbruptly:
	default:
		slog.Warn("errors channel is full, ignoring repeated signal")
	}
}

// installSignalHandler installs the server-wide SIGINT/SIGTERM handler.
// The first signal quits every launched worker gracefully; the second
// reports an abrupt quit.
func (server *Server) installSignalHandler() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	var signalsReceived uint

	go func() {
		for s := range sig {
			slog.Warn("signal received", "signal", s)
			signalsReceived++

			server.workersMu.Lock()
			workers := append([]*Worker(nil), server.workers...)
			server.workersMu.Unlock()

			if signalsReceived < 2 {
				for _, w := range workers {
					w.beginGracefulQuit()
				}
			} else {
				// Abort the program when user hits Ctrl+C second time in a row
				for _, w := range workers {
					w.handleAbruptQuit()
				}
			}
		}
	}()
}

// CustomQueue returns Custom Queue of the running worker process
func (worker *Worker) CustomQueue() string {
	return worker.Queue
}

// Quit tears down the running worker process
func (worker *Worker) Quit() {
	worker.server.GetBroker().StopConsuming()
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *Signature) error {
	// If the task is not registered with this worker, do not continue
	// but only return nil as we do not want to restart the worker process
	if !worker.server.IsTaskRegistered(signature.Name) {
		return nil
	}

	taskFunc, err := worker.server.GetRegisteredTask(signature.Name)
	if err != nil {
		return nil
	}

	// Update task state to RECEIVED
	if err = worker.server.GetBackend().SetStateReceived(context.Background(), signature); err != nil {
		return fmt.Errorf("Set state to 'received' for task %s returned error: %w", signature.UUID, err)
	}

	// Prepare task for processing
	task, err := NewWithSignature(taskFunc, signature)
	// if this failed, it means the task is malformed, probably has invalid
	// signature, go directly to task failed without checking whether to retry
	if err != nil {
		worker.taskFailed(signature, err)
		return err
	}

	// try to extract trace span from headers and add it to the function context
	// so it can be used inside the function if it has context.Context as the first
	// argument. Start a new span if it isn't found.
	taskSpan := StartSpanFromHeaders(signature.Headers, signature.Name)
	defer taskSpan.Finish()
	AnnotateSpanWithSignatureInfo(taskSpan, signature)
	task.Context = opentracing.ContextWithSpan(task.Context, taskSpan)

	// Update task state to STARTED
	if err = worker.server.GetBackend().SetStateStarted(context.Background(), signature); err != nil {
		return fmt.Errorf("Set state to 'started' for task %s returned error: %w", signature.UUID, err)
	}

	//Run handler before the task is called
	if worker.preTaskHandler != nil {
		worker.preTaskHandler(signature)
	}

	//Defer run handler for the end of the task
	if worker.postTaskHandler != nil {
		defer worker.postTaskHandler(signature)
	}

	// Call the task
	results, err := task.Call()
	if err != nil {
		// If a ErrRetryTaskLater was returned from the task,
		// retry the task after specified duration
		var retriableErr ErrRetryTaskLater
		if errors.As(err, &retriableErr) {
			return worker.retryTaskIn(signature, retriableErr.RetryIn())
		}

		// Otherwise, execute default retry logic based on signature.RetryCount
		// and signature.RetryTimeout values
		if signature.RetryCount > 0 {
			return worker.taskRetry(signature)
		}

		return worker.taskFailed(signature, err)
	}

	return worker.taskSucceeded(signature, results)
}

// taskRetry decrements RetryCount counter and republishes the task to the queue
func (worker *Worker) taskRetry(signature *Signature) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(context.Background(), signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %w", signature.UUID, err)
	}

	// Decrement the retry counter, when it reaches 0, we won't retry again
	signature.RetryCount--

	// Increase retry timeout
	signature.RetryTimeout = FibonacciNext(signature.RetryTimeout)

	// Delay task by signature.RetryTimeout seconds
	eta := time.Now().UTC().Add(time.Second * time.Duration(signature.RetryTimeout))
	signature.ETA = &eta

	slog.Warn("task failed, retrying", "task", signature.UUID, "retryInSeconds", signature.RetryTimeout)

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// retryTaskIn republishes the task to the queue with ETA of now + retryIn.Seconds()
func (worker *Worker) retryTaskIn(signature *Signature, retryIn time.Duration) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(context.Background(), signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %w", signature.UUID, err)
	}

	// Delay task by retryIn duration
	eta := time.Now().UTC().Add(retryIn)
	signature.ETA = &eta

	slog.Warn("task failed, retrying", "task", signature.UUID, "retryIn", retryIn)

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// taskSucceeded updates the task state and triggers success callbacks or a
// chord callback if this was the last task of a group with a chord callback
func (worker *Worker) taskSucceeded(signature *Signature, taskResults []*TaskResult) error {
	// Update task state to SUCCESS
	if err := worker.server.GetBackend().SetStateSuccess(context.Background(), signature, taskResults); err != nil {
		return fmt.Errorf("Set state to 'success' for task %s returned error: %w", signature.UUID, err)
	}

	// Log human readable results of the processed task
	var debugResults = "[]"
	results, err := ReflectTaskResults(taskResults)
	if err != nil {
		slog.Warn("failed to reflect task results", "error", err)
	} else {
		debugResults = HumanReadableResults(results)
	}
	slog.Debug("task processed", "task", signature.UUID, "results", debugResults)

	// Trigger success callbacks

	for _, successTask := range signature.OnSuccess {
		// operate on a copy: appending results to the shared template would
		// accumulate arguments across sends
		cb := CopySignature(successTask)
		if cb == nil {
			continue
		}
		if !signature.Immutable {
			// Pass results of the task to success callbacks
			for _, taskResult := range taskResults {
				cb.Args = append(cb.Args, Arg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}

		if _, err := worker.server.SendTask(cb); err != nil {
			// a lost callback must at least be observable
			slog.Error("failed to send success callback", "task", cb.UUID, "error", err)
		}
	}

	// If the task was not part of a group, just return
	if signature.GroupUUID == "" {
		return nil
	}

	// There is no chord callback, just return
	if signature.ChordCallback == nil {
		return nil
	}

	// Check if all task in the group has completed
	groupCompleted, err := worker.server.GetBackend().GroupCompleted(
		context.Background(),
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return fmt.Errorf("Completed check for group %s returned error: %w", signature.GroupUUID, err)
	}

	// If the group has not yet completed, just return
	if !groupCompleted {
		return nil
	}

	// Trigger chord callback
	shouldTrigger, err := worker.server.GetBackend().TriggerChord(context.Background(), signature.GroupUUID)
	if err != nil {
		return fmt.Errorf("Triggering chord for group %s returned error: %w", signature.GroupUUID, err)
	}

	// Chord has already been triggered
	if !shouldTrigger {
		return nil
	}

	// Get task states
	taskStates, err := worker.server.GetBackend().GroupTaskStates(
		context.Background(),
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		slog.Error("failed to get task states for group, chord may not be triggered",
			"group", signature.GroupUUID,
			"taskCount", signature.GroupTaskCount,
			"error", err,
		)
		return nil
	}

	// operate on a copy so the shared callback template is never mutated
	chordCallback := CopySignature(signature.ChordCallback)
	if chordCallback == nil {
		return nil
	}

	for _, taskState := range taskStates {
		if !taskState.IsSuccess() {
			// The chord will never run. Give the callback signature a
			// terminal FAILURE state so chord waiters fail fast instead of
			// polling forever.
			msg := fmt.Sprintf("chord skipped: group task %s did not succeed", taskState.TaskUUID)
			if err := worker.server.GetBackend().SetStateFailure(context.Background(), signature.ChordCallback, msg); err != nil {
				slog.Warn("failed to record chord failure",
					"group", signature.GroupUUID,
					"chord", signature.ChordCallback.UUID,
					"error", err,
				)
			}
			return nil
		}

		if !chordCallback.Immutable {
			// Pass results of the task to the chord callback
			for _, taskResult := range taskState.Results {
				chordCallback.Args = append(chordCallback.Args, Arg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}
	}

	// Send the chord task
	_, err = worker.server.SendTask(chordCallback)
	if err != nil {
		return err
	}

	return nil
}

// taskFailed updates the task state and triggers error callbacks
func (worker *Worker) taskFailed(signature *Signature, taskErr error) error {
	// Update task state to FAILURE
	if err := worker.server.GetBackend().SetStateFailure(context.Background(), signature, taskErr.Error()); err != nil {
		return fmt.Errorf("Set state to 'failure' for task %s returned error: %w", signature.UUID, err)
	}

	if worker.errorHandler != nil {
		worker.errorHandler(taskErr)
	} else {
		slog.Error("failed processing task", "task", signature.UUID, "error", taskErr)
	}

	// Trigger error callbacks
	for _, errorTask := range signature.OnError {
		// operate on a copy so the shared callback template is never mutated
		cb := CopySignature(errorTask)
		if cb == nil {
			continue
		}
		// Pass error as a first argument to error callbacks
		cb.Args = append([]Arg{{
			Type:  "string",
			Value: taskErr.Error(),
		}}, cb.Args...)
		if _, err := worker.server.SendTask(cb); err != nil {
			// a lost callback must at least be observable
			slog.Error("failed to send error callback", "task", cb.UUID, "error", err)
		}
	}

	return nil
}

// SetErrorHandler sets a custom error handler for task errors
// A default behavior is just to log the error after all the retry attempts fail
func (worker *Worker) SetErrorHandler(handler func(err error)) {
	worker.errorHandler = handler
}

// SetPreTaskHandler sets a custom handler func before a job is started
func (worker *Worker) SetPreTaskHandler(handler func(*Signature)) {
	worker.preTaskHandler = handler
}

// SetPostTaskHandler sets a custom handler for the end of a job
func (worker *Worker) SetPostTaskHandler(handler func(*Signature)) {
	worker.postTaskHandler = handler
}

// GetServer returns server
func (worker *Worker) GetServer() *Server {
	return worker.server
}
