// Copyright 2012, 2013 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package worker

import (
	"context"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/juju/clock"
	"github.com/juju/errors"
	"gopkg.in/tomb.v2"
)

const (
	// DefaultRestartDelay holds the default length of time that a worker
	// will wait between exiting and being restarted by a Runner.
	DefaultRestartDelay = 3 * time.Second

	ErrAborted = errors.ConstError("aborted waiting for worker")
	ErrDead    = errors.ConstError("worker runner is not running")
)

// StartFunc is the type of the function that creates a worker.
type StartFunc func(context.Context) (Worker, error)

// Runner runs a set of workers, restarting them as necessary
// when they fail.
type Runner struct {
	tomb     tomb.Tomb
	startc   chan startReq
	stopc    chan string
	donec    chan doneInfo
	startedc chan startInfo

	params RunnerParams

	// isDying is maintained by the run goroutine.
	// When it is dying (whether as a result of being killed or due to a
	// fatal error), all existing workers are killed, no new workers
	// will be started, and the loop will exit when all existing
	// workers have stopped.
	isDying bool

	// finalError is maintained by the run goroutine.
	// finalError holds the error that will be returned
	// when the runner finally exits.
	finalError error

	// mu guards the fields below it. Note that the
	// run goroutine only locks the mutex when
	// it changes workers, not when it reads it. It can do this
	// because it's the only goroutine that changes it.
	mu sync.Mutex

	// workersChangedCond is notified whenever the
	// current workers state changes.
	workersChangedCond sync.Cond

	// workers holds the current set of workers.
	workers map[string]*workerInfo

	// notifyStarted is used only for test synchronisation.
	// As the worker startInfo values are processed, the worker is sent
	// down this channel if this channel is not nil.
	notifyStarted chan<- Worker
}

// workerInfo holds information on one worker id.
type workerInfo struct {
	// worker holds the current Worker instance. This field is
	// guarded by the Runner.mu mutex so it can be inspected
	// by Runner.Worker calls.
	worker Worker

	// The following fields are maintained by the
	// run goroutine.
	labels pprof.LabelSet

	// start holds the function to create the worker.
	// If this is nil, the worker has been stopped
	// and will be removed when its goroutine exits.
	start StartFunc

	// restartDelay holds the length of time that runWorker
	// will wait before calling the start function.
	restartDelay time.Duration

	// stopping holds whether the worker is currently
	// being killed. The runWorker goroutine will
	// still exist while this is true.
	stopping bool

	// done is used to signal when the worker has finished
	// running and is removed from the runner.
	done chan struct{}

	// started holds the time the worker was started.
	started time.Time
}

func (i *workerInfo) status() string {
	if i.stopping && i.worker != nil {
		return "stopping"
	}
	if i.worker == nil {
		return "stopped"
	}
	return "started"
}

type startReq struct {
	ctx   context.Context
	id    string
	start StartFunc
	reply chan error
}

type startInfo struct {
	id     string
	worker Worker
}

type doneInfo struct {
	id  string
	err error
}

// Logger represents the various logging methods used by the runner.
type Logger interface {
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Errorf(string, ...interface{})
}

// Clock represents the methods needed from the clock.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

// RunnerParams holds the parameters for a NewRunner call.
type RunnerParams struct {
	// IsFatal is called when a worker exits. If it returns
	// true, all the other workers will be stopped and the runner
	// itself will finish.
	//
	// If IsFatal is nil, all errors will be treated as fatal.
	IsFatal func(error) bool

	// ShouldRestart is called when a worker exits. If it returns
	// false, the worker will be removed from the runner. All other
	// workers will continue to run.
	ShouldRestart func(error) bool

	// When the runner exits because one or more workers have
	// returned a fatal error, only the most important one,
	// will be returned. MoreImportant should report whether
	// err0 is more important than err1.
	//
	// If MoreImportant is nil, the first error reported will be
	// returned.
	MoreImportant func(err0, err1 error) bool

	// RestartDelay holds the length of time the runner will
	// wait after a worker has exited with a non-fatal error
	// before it is restarted.
	// If this is zero, DefaultRestartDelay will be used.
	RestartDelay time.Duration

	// Clock is used for timekeeping. If it's nil, clock.WallClock
	// will be used.
	Clock Clock

	// Logger is used to provide an implementation for where the logging
	// messages go for the runner. If it's nil, no logging output.
	Logger Logger
}

// NewRunner creates a new Runner.  When a worker finishes, if its error
// is deemed fatal (determined by calling isFatal), all the other workers
// will be stopped and the runner itself will finish.  Of all the fatal errors
// returned by the stopped workers, only the most important one,
// determined by calling moreImportant, will be returned from
// Runner.Wait. Non-fatal errors will not be returned.
//
// The function isFatal(err) returns whether err is a fatal error.  The
// function moreImportant(err0, err1) returns whether err0 is considered
// more important than err1.
func NewRunner(p RunnerParams) *Runner {
	if p.IsFatal == nil {
		p.IsFatal = func(error) bool {
			return true
		}
	}
	if p.ShouldRestart == nil {
		p.ShouldRestart = func(error) bool {
			return true
		}
	}
	if p.MoreImportant == nil {
		p.MoreImportant = func(err0, err1 error) bool {
			return true
		}
	}
	if p.RestartDelay == 0 {
		p.RestartDelay = DefaultRestartDelay
	}
	if p.Clock == nil {
		p.Clock = clock.WallClock
	}
	if p.Logger == nil {
		p.Logger = noopLogger{}
	}

	runner := &Runner{
		startc:   make(chan startReq),
		stopc:    make(chan string),
		donec:    make(chan doneInfo),
		startedc: make(chan startInfo),
		params:   p,
		workers:  make(map[string]*workerInfo),
	}
	runner.workersChangedCond.L = &runner.mu
	runner.tomb.Go(runner.run)
	return runner
}

// StartWorker starts a worker running associated with the given id.
// The startFunc function will be called to create the worker;
// when the worker exits, an AlreadyExists error will be returned.
//
// StartWorker returns ErrDead if the runner is not running.
func (runner *Runner) StartWorker(ctx context.Context, id string, startFunc StartFunc) error {
	// Note: we need the reply channel so that when StartWorker
	// returns, we're guaranteed that the worker is installed
	// when we return, so Worker will see it if called
	// immediately afterwards.
	reply := make(chan error)
	select {
	case runner.startc <- startReq{ctx, id, startFunc, reply}:
		// We're certain to get a reply because the startc channel is synchronous
		// so if we succeed in sending on it, we know that the run goroutine has entered
		// the startc arm of the select, and that calls startWorker (which never blocks)
		// and then immediately sends any error to the reply channel.
		return <-reply
	case <-ctx.Done():
	case <-runner.tomb.Dead():
	}
	return ErrDead
}

// StopWorker stops the worker associated with the given id.
// It does nothing if there is no such worker.
//
// StopWorker returns ErrDead if the runner is not running.
func (runner *Runner) StopWorker(id string) error {
	select {
	case runner.stopc <- id:
		return nil
	case <-runner.tomb.Dead():
	}
	return ErrDead
}

// StopAndRemoveWorker stops the worker and returns any error reported by
// the worker, waiting for the worker to be no longer known to the runner.
// If it was stopped while waiting, StopAndRemoveWorker will return ErrAborted.
//
// StopAndRemoveWorker returns ErrDead if the runner is not running.
func (runner *Runner) StopAndRemoveWorker(id string, abort <-chan struct{}) error {
	w, done, err := runner.workerInfo(id, abort)
	if err != nil {
		return err
	}
	workerErr := Stop(w)

	select {
	case <-abort:
	case <-done:
		return workerErr
	}
	return ErrAborted
}

// Wait implements Worker.Wait
func (runner *Runner) Wait() error {
	return runner.tomb.Wait()
}

// Kill implements Worker.Kill
func (runner *Runner) Kill() {
	runner.params.Logger.Debugf("killing runner %p", runner)
	runner.tomb.Kill(nil)
}

// Worker returns the current worker for the given id.
// If a worker has been started with the given id but is
// not currently available, it will wait until it is available,
// stopping waiting if it receives a value on the stop channel.
//
// If there is no worker started with the given id, Worker
// will return ErrNotFound. If it was aborted while
// waiting, Worker will return ErrAborted. If the runner
// has been killed while waiting, Worker will return ErrDead.
func (runner *Runner) Worker(id string, abort <-chan struct{}) (Worker, error) {
	w, _, err := runner.workerInfo(id, abort)
	return w, err
}

// WorkerNames returns the names of the current workers.
// They are returned in no particular order and they might not exists when
// the Worker request is made.
func (runner *Runner) WorkerNames() []string {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	names := make([]string, 0, len(runner.workers))
	for name := range runner.workers {
		names = append(names, name)
	}
	return names
}

func (runner *Runner) workerInfo(id string, abort <-chan struct{}) (Worker, <-chan struct{}, error) {
	runner.mu.Lock()
	// getWorker returns the current worker for the id
	// and reports an ErrNotFound error if the worker
	// isn't found.
	getWorker := func() (Worker, <-chan struct{}, error) {
		info := runner.workers[id]
		if info == nil {
			// No entry for the id means the worker
			// will never become available.
			return nil, nil, errors.NotFoundf("worker %q", id)
		}
		return info.worker, info.done, nil
	}
	if w, done, err := getWorker(); err != nil || w != nil {
		// The worker is immediately available  (or we know it's
		// not going to become available). No need
		// to block waiting for it.
		runner.mu.Unlock()
		// If it wasn't found, it's possible that's because
		// the whole thing has shut down, so
		// check for dying so that we don't mislead
		// our caller.
		select {
		case <-runner.tomb.Dying():
			return nil, nil, ErrDead
		default:
		}
		return w, done, err
	}
	type workerResult struct {
		w    Worker
		done <-chan struct{}
		err  error
	}
	wc := make(chan workerResult, 1)
	stopped := make(chan struct{})
	go func() {
		defer runner.mu.Unlock()
		for {
			select {
			case <-stopped:
				return
			default:
				// Note: sync.Condition.Wait unlocks the mutex before
				// waiting, then locks it again before returning.
				runner.workersChangedCond.Wait()
				if w, done, err := getWorker(); err != nil || w != nil {
					wc <- workerResult{w, done, err}
					return
				}
			}
		}
	}()
	select {
	case w := <-wc:
		if errors.IsNotFound(w.err) {
			// If it wasn't found, it's possible that's because
			// the whole thing has shut down, so
			// check for dying so that we don't mislead
			// our caller.
			select {
			case <-runner.tomb.Dying():
				return nil, nil, ErrDead
			default:
			}
		}
		return w.w, w.done, w.err
	case <-runner.tomb.Dying():
		return nil, nil, ErrDead
	case <-abort:
	}
	// Stop our wait goroutine.
	// Strictly speaking this can wake up more waiting Worker calls
	// than needed, but this shouldn't be a problem as in practice
	// almost all the time Worker should not need to start the
	// goroutine.
	close(stopped)
	runner.workersChangedCond.Broadcast()
	return nil, nil, ErrAborted
}

func (runner *Runner) run() error {
	tombDying := runner.tomb.Dying()
	for {
		if runner.isDying && len(runner.workers) == 0 {
			return runner.finalError
		}
		select {
		case <-tombDying:
			runner.params.Logger.Infof("runner is dying")
			runner.isDying = true
			runner.killAll()
			tombDying = nil

		case req := <-runner.startc:
			runner.params.Logger.Debugf("start %q", req.id)
			req.reply <- runner.startWorker(req)

		case id := <-runner.stopc:
			runner.params.Logger.Debugf("stop %q", id)
			runner.killWorker(id)

		case info := <-runner.startedc:
			runner.params.Logger.Debugf("%q started", info.id)
			runner.setWorker(info.id, info.worker)
			if runner.notifyStarted != nil {
				runner.notifyStarted <- info.worker
			}
		case info := <-runner.donec:
			runner.params.Logger.Debugf("%q done: %v", info.id, info.err)
			runner.workerDone(info)
		}
		runner.workersChangedCond.Broadcast()
	}
}

// startWorker responds when a worker has been started
// by calling StartWorker.
func (runner *Runner) startWorker(req startReq) error {
	if runner.isDying {
		runner.params.Logger.Infof("ignoring start request for %q when dying", req.id)
		return nil
	}
	info := runner.workers[req.id]
	if info != nil {
		return errors.AlreadyExistsf("worker %q", req.id)
	}

	labels := pprof.Labels(
		"type", "worker",
		"name", workerFuncName(req.start),
		// id is the worker id, which is unique for each worker, this is
		// supplied by the caller of StartWorker.
		"id", req.id,
	)

	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.workers[req.id] = &workerInfo{
		labels:       labels,
		start:        req.start,
		restartDelay: runner.params.RestartDelay,
		started:      runner.params.Clock.Now().UTC(),
		done:         make(chan struct{}, 1),
	}

	pprof.Do(runner.tomb.Context(req.ctx), labels, func(ctx context.Context) {
		go runner.runWorker(ctx, 0, req.id, req.start)
	})
	return nil
}

type panicError interface {
	error
	StackTrace() []string
	Panicked() bool
}

// workerDone responds when a worker has finished or failed
// to start. It maintains the runner.finalError field and
// restarts the worker if necessary.
func (runner *Runner) workerDone(info doneInfo) {
	params := runner.params

	workerInfo := runner.workers[info.id]
	if !workerInfo.stopping && info.err == nil {
		params.Logger.Debugf("removing %q from known workers", info.id)
		runner.removeWorker(info.id, workerInfo.done)
		return
	}
	if info.err != nil {
		errStr := info.err.Error()
		if errWithStack, ok := info.err.(panicError); ok && errWithStack.Panicked() {
			// Panics should always have the full stacktrace in the error log.
			errStr = strings.Join(append([]string{errStr}, errWithStack.StackTrace()...), "\n")
		}

		params.Logger.Debugf("error %q: %s", info.id, errStr)
		if params.IsFatal(info.err) {
			params.Logger.Errorf("fatal error %q: %s", info.id, errStr)
			if runner.finalError == nil || params.MoreImportant(info.err, runner.finalError) {
				runner.finalError = info.err
			}
			runner.removeWorker(info.id, workerInfo.done)
			if !runner.isDying {
				runner.isDying = true
				runner.killAll()
			}
			return
		} else {
			params.Logger.Infof("non-fatal error %q: %s", info.id, errStr)
		}

		if !params.ShouldRestart(info.err) {
			params.Logger.Debugf("removing %q from known workers", info.id)
			runner.removeWorker(info.id, workerInfo.done)
			return
		}
		params.Logger.Errorf("exited %q: %s", info.id, errStr)
	}
	if workerInfo.start == nil {
		params.Logger.Debugf("no restart, removing %q from known workers", info.id)

		// The worker has been deliberately stopped;
		// we can now remove it from the list of workers.
		runner.removeWorker(info.id, workerInfo.done)
		return
	}
	pprof.Do(runner.tomb.Context(context.Background()), workerInfo.labels, func(ctx context.Context) {
		go runner.runWorker(ctx, workerInfo.restartDelay, info.id, workerInfo.start)
	})
	workerInfo.restartDelay = params.RestartDelay
}

// removeWorker removes the worker with the given id from the
// set of current workers. This should only be called when
// the worker is not running.
func (runner *Runner) removeWorker(id string, removed chan<- struct{}) {
	runner.mu.Lock()
	delete(runner.workers, id)
	removed <- struct{}{}
	runner.mu.Unlock()
}

// setWorker sets the worker associated with the given id.
func (runner *Runner) setWorker(id string, w Worker) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	info := runner.workers[id]
	info.worker = w
	if runner.isDying || info.stopping {
		// We're dying or the worker has already been
		// stopped, so kill it already.
		runner.killWorkerLocked(id)
	}
}

// killAll stops all the current workers.
func (runner *Runner) killAll() {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	for id := range runner.workers {
		runner.killWorkerLocked(id)
	}
}

// killWorker stops the worker with the given id, and
// marks it so that it will not start again unless explicitly started
// with StartWorker.
func (runner *Runner) killWorker(id string) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.killWorkerLocked(id)
}

// killWorkerLocked is like killWorker except that it expects
// the runner.mu mutex to be held already.
func (runner *Runner) killWorkerLocked(id string) {
	info := runner.workers[id]
	if info == nil {
		return
	}
	info.stopping = true
	info.start = nil
	if info.worker != nil {
		runner.params.Logger.Debugf("killing %q", id)
		info.worker.Kill()
		info.worker = nil
	} else {
		runner.params.Logger.Debugf("couldn't kill %q, not yet started", id)
	}
}

// runWorker starts the given worker after waiting for the given delay.
func (runner *Runner) runWorker(ctx context.Context, delay time.Duration, id string, start StartFunc) {
	if delay > 0 {
		runner.params.Logger.Infof("restarting %q in %v", id, delay)
		// TODO(rog) provide a way of interrupting this
		// so that it can be stopped when a worker is removed.
		select {
		case <-runner.tomb.Dying():
			runner.donec <- doneInfo{id, nil}
			return
		case <-runner.params.Clock.After(delay):
		}
	}
	runner.params.Logger.Infof("start %q", id)

	// Defensively ensure that we get reasonable behaviour
	// if something calls Goexit inside the worker (this can
	// happen if something calls check.Assert) - if we don't
	// do this, then this will usually turn into a hard-to-find
	// deadlock when the runner is stopped but the runWorker
	// goroutine will never signal that it's finished.
	normal := false
	defer func() {
		if normal {
			return
		}
		// Since normal isn't true, it means that something
		// inside start must have called panic or runtime.Goexit.
		// If it's a panic, we let it panic; if it's called Goexit,
		// we'll just return an error, enabling this functionality
		// to be tested.
		if err := recover(); err != nil {
			panic(err)
		}
		runner.params.Logger.Infof("%q called runtime.Goexit unexpectedly", id)
		runner.donec <- doneInfo{id, errors.Errorf("runtime.Goexit called in running worker - probably inappropriate Assert")}
	}()
	worker, err := start(ctx)
	normal = true

	if err == nil {
		runner.startedc <- startInfo{id, worker}
		err = worker.Wait()
	}
	runner.params.Logger.Infof("stopped %q, err: %v", id, err)
	runner.donec <- doneInfo{id, err}
}

type reporter interface {
	Report() map[string]interface{}
}

// Report implements Reporter.
func (runner *Runner) Report() map[string]interface{} {
	workers := make(map[string]interface{})
	runner.mu.Lock()
	defer runner.mu.Unlock()
	for id, info := range runner.workers {
		worker := info.worker
		workerReport := map[string]interface{}{
			KeyState: info.status(),
		}
		if !info.started.IsZero() {
			workerReport[KeyLastStart] = info.started.Format("2006-01-02 15:04:05")
		}
		if worker != nil {
			if r, ok := worker.(reporter); ok {
				if report := r.Report(); len(report) > 0 {
					workerReport[KeyReport] = report
				}
			}
		}
		workers[id] = workerReport
	}
	return map[string]interface{}{
		"workers": workers,
	}
}

type noopLogger struct{}

func (noopLogger) Debugf(string, ...interface{}) {}
func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Errorf(string, ...interface{}) {}

func workerFuncName(f func(context.Context) (Worker, error)) string {
	name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	if name == "" {
		return ""
	}
	parts := strings.Split(name, ".")

	// Trim the package name and the function name to prevent it taking
	// up too much space in the pprof output.
	trim := 3
	num := len(parts)
	if num < trim {
		return name
	}
	return strings.Join(parts[num-trim:], ".")
}
