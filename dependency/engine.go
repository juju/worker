// Copyright 2015-2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package dependency

import (
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/juju/collections/set"
	"github.com/juju/errors"
	"gopkg.in/tomb.v2"

	"github.com/juju/worker/v2"
)

// Logger represents the various logging methods used by the runner.
type Logger interface {
	Tracef(string, ...interface{})
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Errorf(string, ...interface{})
}

// Clock defines the time methods needed for the engine.
type Clock interface {
	// Now returns the current clock time.
	Now() time.Time

	// After waits for the duration to elapse and then sends the
	// current time on the returned channel.
	After(time.Duration) <-chan time.Time
}

// EngineConfig defines the parameters needed to create a new engine.
type EngineConfig struct {

	// IsFatal returns true when passed an error that should stop
	// the engine. It must not be nil.
	IsFatal IsFatalFunc

	// WorstError returns the more important of two fatal errors
	// passed to it, and is used to determine which fatal error to
	// report when there's more than one. It must not be nil.
	WorstError WorstErrorFunc

	// Filter, if not nil, will modify any fatal error reported from
	// Wait().
	Filter FilterFunc

	// ErrorDelay controls how long the engine waits before starting
	// a worker that stopped with an unknown error. It must not be
	// negative.
	ErrorDelay time.Duration

	// BounceDelay controls how long the engine waits before starting
	// a worker that was deliberately stopped because its dependencies
	// changed. It must not be negative.
	BounceDelay time.Duration

	// BackoffFactor is the value used to multiply the delay time
	// for consecutive start attempts.
	BackoffFactor float64

	// BackoffResetTime determines if a worker is running for longer than
	// this time, then any failure will be treated as a 'fresh' failure.
	// Eg, if this is set to 1 minute, and a service starts and bounces within
	// the first minute of running, then we will exponentially back off the next
	// start. However, if it successfully runs for 2 minutes, then any failure
	// will be immediately retried.
	BackoffResetTime time.Duration

	// MaxDelay is the maximum delay that the engine will wait after
	// the exponential backoff due to consecutive start attempts.
	MaxDelay time.Duration

	// Clock will be a wall clock for production, and test clocks for tests.
	Clock Clock

	// Logger is used to provide an implementation for where the logging
	// messages go for the runner.
	Logger Logger
}

// Validate returns an error if any field is invalid.
func (config *EngineConfig) Validate() error {
	if config.IsFatal == nil {
		return errors.New("IsFatal not specified")
	}
	if config.WorstError == nil {
		return errors.New("WorstError not specified")
	}
	if config.ErrorDelay < 0 {
		return errors.New("ErrorDelay is negative")
	}
	if config.BounceDelay < 0 {
		return errors.New("BounceDelay is negative")
	}
	if config.BackoffFactor != 0 && config.BackoffFactor < 1 {
		return errors.Errorf("BackoffFactor %v must be >= 1", config.BackoffFactor)
	}
	if config.BackoffResetTime < 0 {
		return errors.New("BackoffResetTime is negative")
	}
	if config.MaxDelay < 0 {
		return errors.New("MaxDelay is negative")
	}
	if config.Clock == nil {
		return errors.NotValidf("missing Clock")
	}
	if config.Logger == nil {
		return errors.NotValidf("missing Logger")
	}
	return nil
}

// NewEngine returns an Engine that will maintain any installed Manifolds until
// either the engine is stopped or one of the manifolds' workers returns an error
// that satisfies isFatal. The caller takes responsibility for the returned Engine:
// it's responsible for Kill()ing the Engine when no longer used, and must handle
// any error from Wait().
func NewEngine(config EngineConfig) (*Engine, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Annotatef(err, "invalid config")
	}
	engine := &Engine{
		config: config,

		manifolds:  Manifolds{},
		dependents: map[string][]string{},
		current:    map[string]workerInfo{},

		install: make(chan installTicket),
		started: make(chan startedTicket),
		stopped: make(chan stoppedTicket),
		report:  make(chan reportTicket),
	}
	engine.tomb.Go(engine.loop)
	return engine, nil
}

// Engine maintains workers corresponding to its installed manifolds, and
// restarts them whenever their inputs change.
type Engine struct {

	// config contains values passed in as config when the engine was created.
	config EngineConfig

	// As usual, we use tomb.Tomb to track the lifecycle and error state of the
	// engine worker itself; but we *only* report *internal* errors via the tomb.
	// Fatal errors received from workers are *not* used to kill the tomb; they
	// are tracked separately, and will only be exposed to the client when the
	// engine's tomb has completed its job and encountered no errors.
	tomb tomb.Tomb

	// worstError is used to track the most important fatal error we've received
	// from any manifold. This should be the only place fatal errors are stored;
	// they must *not* be passed into the tomb.
	worstError error

	// manifolds holds the installed manifolds by name.
	manifolds Manifolds

	// dependents holds, for each named manifold, those that depend on it.
	dependents map[string][]string

	// current holds the active worker information for each installed manifold.
	current map[string]workerInfo

	// install, started, report and stopped each communicate requests and changes into
	// the loop goroutine.
	install chan installTicket
	started chan startedTicket
	stopped chan stoppedTicket
	report  chan reportTicket
}

// loop serializes manifold install operations and worker start/stop notifications.
// It's notable for its oneShotDying var, which is necessary because any number of
// start/stop notification could be in flight at the point the engine needs to stop;
// we need to handle all those, and any subsequent messages, until the main loop is
// confident that every worker has stopped. (The usual pattern -- to defer a cleanup
// method to run before tomb.Done in NewEngine -- is not cleanly applicable, because
// it needs to duplicate that start/stop message handling; better to localise that
// in this method.)
func (engine *Engine) loop() error {
	oneShotDying := engine.tomb.Dying()
	for {
		select {
		case <-oneShotDying:
			oneShotDying = nil
			for name := range engine.current {
				engine.requestStop(name)
			}
		case ticket := <-engine.report:
			// This is safe so long as the Report method reads the result.
			ticket.result <- engine.liveReport()
		case ticket := <-engine.install:
			// This is safe so long as the Install method reads the result.
			ticket.result <- engine.gotInstall(ticket.name, ticket.manifold)
		case ticket := <-engine.started:
			engine.gotStarted(ticket.name, ticket.worker, ticket.resourceLog)
		case ticket := <-engine.stopped:
			engine.gotStopped(ticket.name, ticket.error, ticket.resourceLog)
		}
		if engine.isDying() {
			if engine.allOthersStopped() {
				return tomb.ErrDying
			}
		}
	}
}

// Kill is part of the worker.Worker interface.
func (engine *Engine) Kill() {
	engine.tomb.Kill(nil)
}

// Wait is part of the worker.Worker interface.
func (engine *Engine) Wait() error {
	if tombError := engine.tomb.Wait(); tombError != nil {
		return tombError
	}
	err := engine.worstError
	if engine.config.Filter != nil {
		return engine.config.Filter(err)
	}
	return err
}

// Report is part of the Reporter interface.
func (engine *Engine) Report() map[string]interface{} {
	report := make(chan map[string]interface{})
	select {
	case engine.report <- reportTicket{report}:
		// This is safe so long as the loop sends a result.
		return <-report
	case <-engine.tomb.Dead():
		// Note that we don't abort on Dying as we usually would; the
		// oneShotDying approach in loop means that it can continue to
		// process requests until the last possible moment. Only once
		// loop has exited do we fall back to this report.
		report := map[string]interface{}{
			KeyState:     "stopped",
			KeyManifolds: engine.manifoldsReport(),
		}
		if err := engine.Wait(); err != nil {
			report[KeyError] = err.Error()
		}
		return report
	}
}

// liveReport collects and returns information about the engine, its manifolds,
// and their workers. It must only be called from the loop goroutine.
func (engine *Engine) liveReport() map[string]interface{} {
	var reportError error
	state := "started"
	if engine.isDying() {
		state = "stopping"
		if tombError := engine.tomb.Err(); tombError != nil {
			reportError = tombError
		} else {
			reportError = engine.worstError
		}
	}
	report := map[string]interface{}{
		KeyState:     state,
		KeyManifolds: engine.manifoldsReport(),
	}
	if reportError != nil {
		report[KeyError] = reportError.Error()
	}
	return report
}

// manifoldsReport collects and returns information about the engine's manifolds
// and their workers. Until the tomb is Dead, it should only be called from the
// loop goroutine; after that, it's goroutine-safe.
func (engine *Engine) manifoldsReport() map[string]interface{} {
	manifolds := map[string]interface{}{}
	for name, info := range engine.current {
		report := map[string]interface{}{
			KeyState:  info.state(),
			KeyInputs: engine.manifolds[name].Inputs,
		}
		if info.startCount > 0 {
			report[KeyStartCount] = info.startCount
		}
		if !info.startedTime.IsZero() {
			report[KeyLastStart] = info.startedTime.Format("2006-01-02 15:04:05")
		}
		if info.err != nil {
			report[KeyError] = info.err.Error()
		}
		if reporter, ok := info.worker.(Reporter); ok {
			if reporter != engine {
				report[KeyReport] = reporter.Report()
			}
		}
		manifolds[name] = report
	}
	return manifolds
}

// Install is part of the Engine interface.
func (engine *Engine) Install(name string, manifold Manifold) error {
	result := make(chan error)
	select {
	case <-engine.tomb.Dying():
		return errors.New("engine is shutting down")
	case engine.install <- installTicket{name, manifold, result}:
		// This is safe so long as the loop sends a result.
		return <-result
	}
}

// gotInstall handles the params originally supplied to Install. It must only be
// called from the loop goroutine.
func (engine *Engine) gotInstall(name string, manifold Manifold) error {
	engine.config.Logger.Tracef("installing %q manifold...", name)
	if _, found := engine.manifolds[name]; found {
		return errors.Errorf("%q manifold already installed", name)
	}
	if err := engine.checkAcyclic(name, manifold); err != nil {
		return errors.Annotatef(err, "cannot install %q manifold", name)
	}
	engine.manifolds[name] = manifold
	for _, input := range manifold.Inputs {
		engine.dependents[input] = append(engine.dependents[input], name)
	}
	engine.current[name] = workerInfo{}
	engine.requestStart(name, 0)
	return nil
}

// uninstall removes the named manifold from the engine's records.
func (engine *Engine) uninstall(name string) {
	// Note that we *don't* want to remove dependents[name] -- all those other
	// manifolds do still depend on this, and another manifold with the same
	// name might be installed in the future -- but we do want to remove the
	// named manifold from all *values* in the dependents map.
	for dName, dependents := range engine.dependents {
		depSet := set.NewStrings(dependents...)
		depSet.Remove(name)
		engine.dependents[dName] = depSet.Values()
	}
	delete(engine.current, name)
	delete(engine.manifolds, name)
}

// checkAcyclic returns an error if the introduction of the supplied manifold
// would cause the dependency graph to contain cycles.
func (engine *Engine) checkAcyclic(name string, manifold Manifold) error {
	manifolds := Manifolds{name: manifold}
	for name, manifold := range engine.manifolds {
		manifolds[name] = manifold
	}
	return Validate(manifolds)
}

// requestStart invokes a runWorker goroutine for the manifold with the supplied
// name. It must only be called from the loop goroutine.
func (engine *Engine) requestStart(name string, delay time.Duration) {

	// Check preconditions.
	manifold, found := engine.manifolds[name]
	if !found {
		engine.tomb.Kill(errors.Errorf("fatal: unknown manifold %q", name))
	}

	// Copy current info and check more preconditions.
	info := engine.current[name]
	if !info.stopped() {
		engine.tomb.Kill(errors.Errorf("fatal: trying to start a second %q manifold worker", name))
	}

	// Final check that we're not shutting down yet...
	if engine.isDying() {
		engine.config.Logger.Tracef("not starting %q manifold worker (shutting down)", name)
		return
	}

	// ...then update the info, copy it back to the engine, and start a worker
	// goroutine based on current known state.
	info.starting = true
	info.startAttempts++
	info.err = nil
	info.abort = make(chan struct{})
	engine.current[name] = info
	context := engine.context(name, manifold.Inputs, info.abort)

	// Always fuzz the delay a bit to help randomise the order of workers starting,
	// which should make bugs more obvious
	if delay > time.Duration(0) {
		if engine.config.BackoffFactor > 0 {
			// Use the float64 values for max comparison. Otherwise when casting
			// the float back to a duration we hit the int64 max which is negative.
			maxDelay := float64(engine.config.MaxDelay)
			floatDelay := float64(delay) * math.Pow(engine.config.BackoffFactor, float64(info.recentErrors-1))
			if engine.config.MaxDelay > 0 && floatDelay > maxDelay {
				delay = engine.config.MaxDelay
			} else {
				delay = time.Duration(floatDelay)
			}
		}
		// Fuzz to ±10% of final duration.
		fuzz := rand.Float64()*0.2 + 0.9
		delay = time.Duration(float64(delay) * fuzz).Round(time.Millisecond)
	}

	go engine.runWorker(name, delay, manifold.Start, context)
}

// context returns a context backed by a snapshot of current
// worker state, restricted to those workers declared in inputs. It must only
// be called from the loop goroutine; see inside for a detailed discussion of
// why we took this approach.
func (engine *Engine) context(name string, inputs []string, abort <-chan struct{}) *context {
	// We snapshot the resources available at invocation time, rather than adding an
	// additional communicate-resource-request channel. The latter approach is not
	// unreasonable... but is prone to inelegant scrambles when starting several
	// dependent workers at once. For example:
	//
	//  * Install manifold A; loop starts worker A
	//  * Install manifold B; loop starts worker B
	//  * A communicates its worker back to loop; main thread bounces B
	//  * B asks for A, gets A, doesn't react to bounce (*)
	//  * B communicates its worker back to loop; loop kills it immediately in
	//    response to earlier bounce
	//  * loop starts worker B again, now everything's fine; but, still, yuck.
	//    This is not a happy path to take by default.
	//
	// The problem, of course, is in the (*); the main thread *does* know that B
	// needs to bounce soon anyway, and it *could* communicate that fact back via
	// an error over a channel back into context.Get; the StartFunc could then
	// just return (say) that ErrResourceChanged and avoid the hassle of creating
	// a worker. But that adds a whole layer of complexity (and unpredictability
	// in tests, which is not much fun) for very little benefit.
	//
	// In the analogous scenario with snapshotted dependencies, we see a happier
	// picture at startup time:
	//
	//  * Install manifold A; loop starts worker A
	//  * Install manifold B; loop starts worker B with empty resource snapshot
	//  * A communicates its worker back to loop; main thread bounces B
	//  * B's StartFunc asks for A, gets nothing, returns ErrMissing
	//  * loop restarts worker B with an up-to-date snapshot, B works fine
	//
	// We assume that, in the common case, most workers run without error most
	// of the time; and, thus, that the vast majority of worker startups will
	// happen as an agent starts. Furthermore, most of them will have simple
	// hard dependencies, and their Start funcs will be easy to write; the only
	// components that may be impacted by such a strategy will be those workers
	// which still want to run (with reduced functionality) with some dependency
	// unmet.
	//
	// Those may indeed suffer the occasional extra bounce as the system comes
	// to stability as it starts, or after a change; but workers *must* be
	// written for resilience in the face of arbitrary bounces *anyway*, so it
	// shouldn't be harmful.
	outputs := map[string]OutputFunc{}
	workers := map[string]worker.Worker{}
	for _, resourceName := range inputs {
		outputs[resourceName] = engine.manifolds[resourceName].Output
		workers[resourceName] = engine.current[resourceName].worker
	}
	return &context{
		clientName: name,
		abort:      abort,
		expired:    make(chan struct{}),
		workers:    workers,
		outputs:    outputs,
		logger:     engine.config.Logger,
	}
}

var errAborted = errors.New("aborted before delay elapsed")

// runWorker starts the supplied manifold's worker and communicates it back to the
// loop goroutine; waits for worker completion; and communicates any error encountered
// back to the loop goroutine. It must not be run on the loop goroutine.
func (engine *Engine) runWorker(name string, delay time.Duration, start StartFunc, context *context) {

	startAfterDelay := func() (worker.Worker, error) {
		// NOTE: the context will expire *after* the worker is started.
		// This is tolerable because
		//  1) we'll still correctly block access attempts most of the time
		//  2) failing to block them won't cause data races anyway
		//  3) it's not worth complicating the interface for every client just
		//     to eliminate the possibility of one harmlessly dumb interaction.
		defer context.expire()
		engine.config.Logger.Tracef("starting %q manifold worker in %s...", name, delay)
		select {
		case <-engine.tomb.Dying():
			return nil, errAborted
		case <-context.Abort():
			return nil, errAborted
		case <-engine.config.Clock.After(delay):
		}
		engine.config.Logger.Tracef("starting %q manifold worker", name)
		return start(context)
	}

	startWorkerAndWait := func() error {
		worker, err := startAfterDelay()
		switch errors.Cause(err) {
		case errAborted:
			return errAborted
		case nil:
			engine.config.Logger.Tracef("running %q manifold worker", name)
		default:
			return err
		}
		select {
		case <-engine.tomb.Dying():
			engine.config.Logger.Tracef("stopping %q manifold worker (shutting down)", name)
			// Doesn't matter whether worker == engine: if we're already Dying
			// then cleanly Kill()ing ourselves again won't hurt anything.
			worker.Kill()
		case engine.started <- startedTicket{name, worker, context.accessLog}:
			engine.config.Logger.Tracef("registered %q manifold worker", name)
		}
		if worker == engine {
			// We mustn't Wait() for ourselves to complete here, or we'll
			// deadlock. But we should wait until we're Dying, because we
			// need this func to keep running to keep the self manifold
			// accessible as a resource.
			<-engine.tomb.Dying()
			return tomb.ErrDying
		}

		return worker.Wait()
	}

	// We may or may not send on started, but we *must* send on stopped.
	engine.stopped <- stoppedTicket{name, startWorkerAndWait(), context.accessLog}
}

// gotStarted updates the engine to reflect the creation of a worker. It must
// only be called from the loop goroutine.
func (engine *Engine) gotStarted(name string, worker worker.Worker, resourceLog []resourceAccess) {
	// Copy current info; check preconditions and abort the workers if we've
	// already been asked to stop it.
	info := engine.current[name]
	switch {
	case info.worker != nil:
		engine.tomb.Kill(errors.Errorf("fatal: unexpected %q manifold worker start", name))
		fallthrough
	case info.stopping, engine.isDying():
		engine.config.Logger.Tracef("%q manifold worker no longer required", name)
		worker.Kill()
	default:
		// It's fine to use this worker; update info and copy back.
		info.worker = worker
		info.starting = false
		info.startCount++
		// Reset the start attempts after a successful start.
		info.startAttempts = 0
		info.resourceLog = resourceLog
		info.startedTime = engine.config.Clock.Now().UTC()
		engine.config.Logger.Debugf("%q manifold worker started at %v", name, info.startedTime)
		engine.current[name] = info

		// Any manifold that declares this one as an input needs to be restarted.
		engine.bounceDependents(name)
	}
}

type stackTracer interface {
	StackTrace() []string
}

// gotStopped updates the engine to reflect the demise of (or failure to create)
// a worker. It must only be called from the loop goroutine.
func (engine *Engine) gotStopped(name string, err error, resourceLog []resourceAccess) {
	// Copy current info and check for reasons to stop the engine.
	info := engine.current[name]

	switch errors.Cause(err) {
	case nil:
		engine.config.Logger.Debugf("%q manifold worker completed successfully", name)
		info.recentErrors = 0
	case errAborted:
		// The start attempt was aborted, so we haven't really started.
		engine.config.Logger.Tracef("%q manifold worker bounced while starting", name)
		// If we have been aborted while trying to start, we are more likely
		// to be able to start, so reset the start attempts.
		info.startAttempts = 0
		info.recentErrors = 1
	case ErrMissing:
		engine.config.Logger.Tracef("%q manifold worker failed to start: %v", name, err)
		// missing a dependency does (not?) trigger exponential backoff
		info.recentErrors = 1
	default:
		if tracer, ok := err.(stackTracer); ok {
			engine.config.Logger.Debugf("%q manifold worker stopped: %v\nstack trace:\n%s",
				name, err, strings.Join(tracer.StackTrace(), "\n"))
		} else {
			engine.config.Logger.Debugf("%q manifold worker stopped: %v", name, err)
		}
		now := engine.config.Clock.Now().UTC()
		timeSinceStarted := now.Sub(info.startedTime)
		// startedTime is Zero, then we haven't even successfully started, so treat
		// it the same way as a successful start followed by a quick failure.
		if info.startedTime.IsZero() || timeSinceStarted < engine.config.BackoffResetTime {
			info.recentErrors++
		} else {
			// we were able to successfully run for long enough to reset the attempt count
			info.recentErrors = 1
		}
	}

	if filter := engine.manifolds[name].Filter; filter != nil {
		err = filter(err)
	}

	if info.stopped() {
		engine.tomb.Kill(errors.Errorf("fatal: unexpected %q manifold worker stop", name))
	} else if engine.config.IsFatal(err) {
		engine.worstError = engine.config.WorstError(err, engine.worstError)
		engine.tomb.Kill(nil)
	}

	// Reset engine info; and bail out if we can be sure there's no need to bounce.
	engine.current[name] = workerInfo{
		err:         err,
		resourceLog: resourceLog,
		// Keep the start count and start attempts but clear the start timestamps.
		startAttempts: info.startAttempts,
		startCount:    info.startCount,
		recentErrors:  info.recentErrors,
	}
	if engine.isDying() {
		engine.config.Logger.Tracef("permanently stopped %q manifold worker (shutting down)", name)
		return
	}

	// If we told the worker to stop, we should start it again immediately,
	// whatever else happened.
	if info.stopping {
		engine.requestStart(name, engine.config.BounceDelay)
	} else {
		// If we didn't stop it ourselves, we need to interpret the error.
		switch errors.Cause(err) {
		case nil, errAborted:
			// Nothing went wrong; the task completed successfully. Nothing
			// needs to be done (unless the inputs change, in which case it
			// gets to check again).
		case ErrMissing:
			// The task can't even start with the current state. Nothing more
			// can be done (until the inputs change, in which case we retry
			// anyway).
		case ErrBounce:
			// The task exited but wanted to restart immediately.
			engine.requestStart(name, engine.config.BounceDelay)
		case ErrUninstall:
			// The task should never run again, and can be removed completely.
			engine.uninstall(name)
		default:
			// TODO(achilleasa): checking against strings is flakey as it can break
			// if the error message changes in the future; a better approach would
			// be to refactor juju/errors to include a typed TryAgain error with a
			// IsTryAgain() helper.
			logFn := engine.config.Logger.Errorf
			if strings.Contains(err.Error(), "try again") {
				// We have been asked to retry; since we will do that anyway
				// with no need of human intervention we should not log this
				// at the ERROR level but rather switch to INFO instead.
				logFn = engine.config.Logger.Infof
			}

			// Something went wrong but we don't know what. Try again soon.
			logFn("%q manifold worker returned unexpected error: %v", name, err)
			engine.requestStart(name, engine.config.ErrorDelay)
		}
	}

	// Manifolds that declared a dependency on this one only need to be notified
	// if the worker has changed; if it was already nil, nobody needs to know.
	if info.worker != nil {
		engine.bounceDependents(name)
	}
}

// requestStop ensures that any running or starting worker will be stopped in the
// near future. It must only be called from the loop goroutine.
func (engine *Engine) requestStop(name string) {

	// If already stopping or stopped, just don't do anything.
	info := engine.current[name]
	if info.stopping || info.stopped() {
		return
	}

	// Update info, kill worker if present, and copy info back to engine.
	info.stopping = true
	if info.abort != nil {
		close(info.abort)
		info.abort = nil
	}
	if info.worker != nil {
		info.worker.Kill()
	}
	engine.current[name] = info
}

// isDying returns true if the engine is shutting down. It's safe to call it
// from any goroutine.
func (engine *Engine) isDying() bool {
	select {
	case <-engine.tomb.Dying():
		return true
	default:
		return false
	}
}

// allOthersStopped returns true if no workers (other than the engine itself,
// if it happens to have been injected) are running or starting. It must only
// be called from the loop goroutine.
func (engine *Engine) allOthersStopped() bool {
	for _, info := range engine.current {
		if !info.stopped() && info.worker != engine {
			return false
		}
	}
	return true
}

// bounceDependents starts every stopped dependent of the named manifold, and
// stops every started one (and trusts the rest of the engine to restart them).
// It must only be called from the loop goroutine.
func (engine *Engine) bounceDependents(name string) {
	engine.config.Logger.Tracef("restarting dependents of %q manifold", name)
	for _, dependentName := range engine.dependents[name] {
		if engine.current[dependentName].stopped() {
			engine.requestStart(dependentName, engine.config.BounceDelay)
		} else {
			engine.requestStop(dependentName)
		}
	}
}

// workerInfo stores what an engine's loop goroutine needs to know about the
// worker for a given Manifold.
type workerInfo struct {
	starting    bool
	stopping    bool
	abort       chan struct{}
	worker      worker.Worker
	err         error
	resourceLog []resourceAccess

	startedTime   time.Time
	startCount    int
	startAttempts int
	recentErrors  int
}

// stopped returns true unless the worker is either assigned or starting.
func (info workerInfo) stopped() bool {
	switch {
	case info.worker != nil:
		return false
	case info.starting:
		return false
	}
	return true
}

// state returns the latest known state of the worker, for use in reports.
func (info workerInfo) state() string {
	switch {
	case info.starting:
		return "starting"
	case info.stopping:
		return "stopping"
	case info.worker != nil:
		return "started"
	}
	return "stopped"
}

// installTicket is used by engine to induce installation of a named manifold
// and pass on any errors encountered in the process.
type installTicket struct {
	name     string
	manifold Manifold
	result   chan<- error
}

// startedTicket is used by engine to notify the loop of the creation of the
// worker for a particular manifold.
type startedTicket struct {
	name        string
	worker      worker.Worker
	resourceLog []resourceAccess
}

// stoppedTicket is used by engine to notify the loop of the demise of (or
// failure to create) the worker for a particular manifold.
type stoppedTicket struct {
	name        string
	error       error
	resourceLog []resourceAccess
}

// reportTicket is used by the engine to notify the loop that a status report
// should be generated.
type reportTicket struct {
	result chan map[string]interface{}
}
