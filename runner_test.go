// Copyright 2012, 2013 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package worker_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/clock/testclock"
	"github.com/juju/errors"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/tomb.v2"

	"github.com/juju/worker/v4"
)

type RunnerSuite struct {
	testing.IsolationSuite
}

var _ = gc.Suite(&RunnerSuite{})

// Ensure that the Runner supports the Reporter interface.
var _ worker.Reporter = (*worker.Runner)(nil)

const (
	shortWait = 100 * time.Millisecond
	longWait  = 5 * time.Second
)

func (*RunnerSuite) TestOneWorkerStart(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: time.Millisecond,
	})
	starter := newTestWorkerStarter()
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)
	c.Assert(runner.WorkerNames(), jc.SameContents, []string{"id"})

	c.Assert(worker.Stop(runner), gc.IsNil)
	starter.assertStarted(c, false)
	c.Assert(runner.WorkerNames(), jc.SameContents, []string{})
}

func (*RunnerSuite) TestOneWorkerFinish(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: time.Millisecond,
	})
	starter := newTestWorkerStarter()
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)

	starter.die <- nil
	starter.assertStarted(c, false)
	starter.assertNeverStarted(c, time.Millisecond)

	c.Assert(worker.Stop(runner), gc.IsNil)
	c.Assert(runner.WorkerNames(), jc.SameContents, []string{})
}

func (*RunnerSuite) TestOneWorkerRestart(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: time.Millisecond,
	})
	starter := newTestWorkerStarter()
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)

	// Check it restarts a few times time.
	for i := 0; i < 3; i++ {
		starter.die <- fmt.Errorf("an error")
		starter.assertStarted(c, false)
		starter.assertStarted(c, true)
	}

	c.Assert(worker.Stop(runner), gc.IsNil)
	starter.assertStarted(c, false)
}

func (*RunnerSuite) TestStopAndWaitWorker(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: 3 * time.Second,
	})
	starter := newTestWorkerStarter()
	starter.stopWait = make(chan struct{})

	// Start a worker, and wait for it.
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)

	errc := make(chan error)
	go func() {
		errc <- runner.StopAndRemoveWorker("id", nil)
	}()

	select {
	case <-time.After(testing.ShortWait):
	case err := <-errc:
		c.Fatalf("got unexpected result, error %q", err)
	}

	starter.stopWait <- struct{}{}
	select {
	case <-time.After(testing.LongWait):
		c.Fatalf("timed out waiting for worker to stop")
	case err := <-errc:
		c.Assert(err, jc.ErrorIsNil)
	}
	// Stop again returns not found.
	err = runner.StopAndRemoveWorker("id", nil)
	c.Assert(err, jc.Satisfies, errors.IsNotFound)
}

func (*RunnerSuite) TestStopAndWaitWorkerReturnsWorkerError(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: 3 * time.Second,
	})
	starter := newTestWorkerStarter()
	starter.stopWait = make(chan struct{})
	starter.killErr = errors.New("boom")

	// Start a worker, and wait for it.
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)

	errc := make(chan error)
	go func() {
		errc <- runner.StopAndRemoveWorker("id", nil)
	}()

	select {
	case <-time.After(testing.ShortWait):
	case err := <-errc:
		c.Fatalf("got unexpected result, error %q", err)
	}

	starter.stopWait <- struct{}{}
	select {
	case <-time.After(100 * testing.LongWait):
		c.Fatalf("timed out waiting for worker to stop")
	case err := <-errc:
		c.Assert(err, gc.Equals, starter.killErr)
	}
}

func (*RunnerSuite) TestStopAndWaitWorkerWithAbort(c *gc.C) {
	t0 := time.Now()
	clock := testclock.NewClock(t0)
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: time.Second,
		Clock:        clock,
	})
	defer worker.Stop(runner)
	starter := newTestWorkerStarter()
	starter.startErr = errors.Errorf("test error")
	runner.StartWorker(context.Background(), "id", starter.start)

	// Wait for the runner start waiting for the restart delay.
	select {
	case <-clock.Alarms():
	case <-time.After(longWait):
		c.Fatalf("runner never slept")
	}

	errc := make(chan error, 1)
	stop := make(chan struct{})
	go func() {
		errc <- runner.StopAndRemoveWorker("id", stop)
	}()
	select {
	case err := <-errc:
		c.Fatalf("got unexpected result, error %q", err)
	case <-time.After(shortWait):
	}

	close(stop)
	select {
	case err := <-errc:
		c.Assert(errors.Is(err, worker.ErrAborted), jc.IsTrue)
	case <-time.After(longWait):
		c.Fatalf("timed out waiting for worker")
	}
}

func (*RunnerSuite) TestOneWorkerStartFatalError(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	starter := newTestWorkerStarter()
	starter.startErr = errors.New("cannot start test task")
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	err = runner.Wait()
	c.Assert(err, gc.Equals, starter.startErr)
}

func (*RunnerSuite) TestOneWorkerDieFatalError(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	starter := newTestWorkerStarter()
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)
	dieErr := errors.New("error when running")
	starter.die <- dieErr
	err = runner.Wait()
	c.Assert(err, gc.Equals, dieErr)
	starter.assertStarted(c, false)
}

func (*RunnerSuite) TestOneWorkerStartStop(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	starter := newTestWorkerStarter()
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)
	err = runner.StopWorker("id")
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, false)
	c.Assert(worker.Stop(runner), gc.IsNil)
}

func (*RunnerSuite) TestOneWorkerStopFatalError(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	starter := newTestWorkerStarter()
	starter.stopErr = errors.New("stop error")
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)
	err = runner.StopWorker("id")
	c.Assert(err, jc.ErrorIsNil)
	err = runner.Wait()
	c.Assert(err, gc.Equals, starter.stopErr)
}

func (*RunnerSuite) TestWorkerStartWhenRunningOrStopping(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: 3 * time.Second,
	})
	starter := newTestWorkerStarter()
	starter.stopWait = make(chan struct{})

	// Start a worker, and wait for it.
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)

	err = runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.Satisfies, errors.IsAlreadyExists)

	// XXX the above does not imply the *runner* knows it's started.
	// voodoo sleep ahoy!
	time.Sleep(shortWait)

	// Stop the worker, which will block...
	err = runner.StopWorker("id")
	c.Assert(err, jc.ErrorIsNil)

	// While it's still blocked, try to start another.
	err = runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.Satisfies, errors.IsAlreadyExists)
}

func (*RunnerSuite) TestOneWorkerRestartDelay(c *gc.C) {
	const delay = 100 * time.Millisecond
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: delay,
	})
	starter := newTestWorkerStarter()
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)
	starter.die <- fmt.Errorf("non-fatal error")
	starter.assertStarted(c, false)
	t0 := time.Now()
	starter.assertStarted(c, true)
	restartDuration := time.Since(t0)
	if restartDuration < delay {
		c.Fatalf("restart delay was not respected; got %v want %v", restartDuration, delay)
	}
	c.Assert(worker.Stop(runner), gc.IsNil)
}

func (*RunnerSuite) TestOneWorkerShouldRestart(c *gc.C) {
	const delay = 100 * time.Millisecond
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:       noneFatal,
		ShouldRestart: func(err error) bool { return false },
		RestartDelay:  delay,
	})
	starter := newTestWorkerStarter()
	err := runner.StartWorker(context.Background(), "id", starter.start)
	c.Assert(err, jc.ErrorIsNil)
	starter.assertStarted(c, true)
	starter.die <- fmt.Errorf("non-fatal error")
	starter.assertStarted(c, false)
	starter.assertNeverStarted(c, delay)
	w, err := runner.Worker("id", nil)
	c.Assert(err, jc.Satisfies, errors.IsNotFound)
	c.Assert(w, gc.Equals, nil)
	c.Assert(worker.Stop(runner), gc.IsNil)
}

type errorLevel int

func (e errorLevel) Error() string {
	return fmt.Sprintf("error with importance %d", e)
}

func (*RunnerSuite) TestErrorImportance(c *gc.C) {
	moreImportant := func(err0, err1 error) bool {
		return err0.(errorLevel) > err1.(errorLevel)
	}
	id := func(i int) string { return fmt.Sprint(i) }
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:       allFatal,
		MoreImportant: moreImportant,
		RestartDelay:  time.Millisecond,
	})
	for i := 0; i < 10; i++ {
		starter := newTestWorkerStarter()
		starter.stopErr = errorLevel(i)
		err := runner.StartWorker(context.Background(), id(i), starter.start)
		c.Assert(err, jc.ErrorIsNil)
	}
	err := runner.StopWorker(id(4))
	c.Assert(err, jc.ErrorIsNil)
	err = runner.Wait()
	c.Assert(err, gc.Equals, errorLevel(9))
}

func (*RunnerSuite) TestStartWorkerWhenDead(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	c.Assert(worker.Stop(runner), gc.IsNil)
	c.Assert(runner.StartWorker(context.Background(), "foo", nil), gc.Equals, worker.ErrDead)
	c.Assert(runner.WorkerNames(), jc.SameContents, []string{})
}

func (*RunnerSuite) TestStopWorkerWhenDead(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	c.Assert(worker.Stop(runner), gc.IsNil)
	c.Assert(runner.StopWorker("foo"), gc.Equals, worker.ErrDead)
	c.Assert(runner.WorkerNames(), jc.SameContents, []string{})
}

func (*RunnerSuite) TestAllWorkersStoppedWhenOneDiesWithFatalError(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	var starters []*testWorkerStarter
	for i := 0; i < 10; i++ {
		starter := newTestWorkerStarter()
		err := runner.StartWorker(context.Background(), fmt.Sprint(i), starter.start)
		c.Assert(err, jc.ErrorIsNil)
		starters = append(starters, starter)
	}
	for i, starter := range starters {
		starter.assertStarted(c, true)
		c.Assert(runner.WorkerNames(), Contains, fmt.Sprint(i))
	}
	dieErr := errors.New("fatal error")
	starters[4].die <- dieErr
	err := runner.Wait()
	c.Assert(err, gc.Equals, dieErr)
	for _, starter := range starters {
		starter.assertStarted(c, false)
	}
	c.Assert(runner.WorkerNames(), jc.SameContents, []string{})
}

func (*RunnerSuite) TestFatalErrorWhileStarting(c *gc.C) {
	// Original deadlock problem that this tests for:
	// A worker dies with fatal error while another worker
	// is inside start(). runWorker can't send startInfo on startedc.
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})

	slowStarter := newTestWorkerStarter()
	// make the startNotify channel synchronous so
	// we can delay the start indefinitely.
	slowStarter.startNotify = make(chan bool)

	err := runner.StartWorker(context.Background(), "slow starter", slowStarter.start)
	c.Assert(err, jc.ErrorIsNil)

	fatalStarter := newTestWorkerStarter()
	fatalStarter.startErr = fmt.Errorf("a fatal error")

	err = runner.StartWorker(context.Background(), "fatal worker", fatalStarter.start)
	c.Assert(err, jc.ErrorIsNil)

	// Wait for the runner loop to react to the fatal
	// error and go into final shutdown mode.
	time.Sleep(10 * time.Millisecond)

	// At this point, the loop is in shutdown mode, but the
	// slowStarter's worker is still in its start function.
	// When the start function continues (the first assertStarted
	// allows that to happen) and returns the new Worker,
	// runWorker will try to send it on runner.startedc.
	// This test makes sure that succeeds ok.

	slowStarter.assertStarted(c, true)
	slowStarter.assertStarted(c, false)
	err = runner.Wait()
	c.Assert(err, gc.Equals, fatalStarter.startErr)
}

func (*RunnerSuite) TestFatalErrorWhileSelfStartWorker(c *gc.C) {
	// Original deadlock problem that this tests for:
	// A worker tries to call StartWorker in its start function
	// at the same time another worker dies with a fatal error.
	// It might not be able to send on startc.
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})

	selfStarter := newTestWorkerStarter()
	// make the startNotify channel synchronous so
	// we can delay the start indefinitely.
	selfStarter.startNotify = make(chan bool)
	selfStarter.hook = func() {
		runner.StartWorker(context.Background(), "another", func(context.Context) (worker.Worker, error) {
			return nil, fmt.Errorf("no worker started")
		})
	}
	err := runner.StartWorker(context.Background(), "self starter", selfStarter.start)
	c.Assert(err, jc.ErrorIsNil)

	fatalStarter := newTestWorkerStarter()
	fatalStarter.startErr = fmt.Errorf("a fatal error")

	err = runner.StartWorker(context.Background(), "fatal worker", fatalStarter.start)
	c.Assert(err, jc.ErrorIsNil)

	// Wait for the runner loop to react to the fatal
	// error and go into final shutdown mode.
	time.Sleep(10 * time.Millisecond)

	// At this point, the loop is in shutdown mode, but the
	// selfStarter's worker is still in its start function.
	// When the start function continues (the first assertStarted
	// allows that to happen) it will try to create a new
	// worker. This failed in an earlier version of the code because the
	// loop was not ready to receive start requests.

	selfStarter.assertStarted(c, true)
	selfStarter.assertStarted(c, false)
	err = runner.Wait()
	c.Assert(err, gc.Equals, fatalStarter.startErr)
}

func (*RunnerSuite) TestWorkerWithNoWorker(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	w, err := runner.Worker("id", nil)
	c.Assert(err, jc.Satisfies, errors.IsNotFound)
	c.Assert(w, gc.Equals, nil)
	c.Assert(runner.WorkerNames(), jc.SameContents, []string{})
}

func (*RunnerSuite) TestWorkerWithWorkerImmediatelyAvailable(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	starter := newTestWorkerStarter()
	runner.StartWorker(context.Background(), "id", starter.start)

	// Wait long enough to be reasonably confident that the
	// worker has been started and registered.
	time.Sleep(10 * time.Millisecond)

	stop := make(chan struct{})
	close(stop)
	w, err := runner.Worker("id", stop)
	c.Assert(err, gc.Equals, nil)
	c.Assert(w, gc.NotNil)
	c.Assert(w.(*testWorker).starter, gc.Equals, starter)
}

func (*RunnerSuite) TestWorkerWithWorkerNotImmediatelyAvailable(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      allFatal,
		RestartDelay: time.Millisecond,
	})
	defer worker.Stop(runner)
	starter := newTestWorkerStarter()
	startCh := make(chan struct{})
	runner.StartWorker(context.Background(), "id", func(ctx context.Context) (worker.Worker, error) {
		startCh <- struct{}{}
		<-startCh
		return starter.start(ctx)
	})
	<-startCh
	// The start function has been called but we know the
	// worker hasn't been returned yet.
	wc := make(chan worker.Worker)
	go func() {
		w, err := runner.Worker("id", nil)
		c.Check(err, gc.Equals, nil)
		wc <- w
	}()
	// Sleep long enough that we're pretty sure that Worker
	// will be blocked.
	time.Sleep(10 * time.Millisecond)
	close(startCh)
	select {
	case w := <-wc:
		c.Assert(w, gc.NotNil)
		c.Assert(w.(*testWorker).starter, gc.Equals, starter)
	case <-time.After(longWait):
		c.Fatalf("timed out waiting for worker")
	}
}

func (*RunnerSuite) TestWorkerWithAbort(c *gc.C) {
	t0 := time.Now()
	clock := testclock.NewClock(t0)
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: time.Second,
		Clock:        clock,
	})
	defer worker.Stop(runner)
	starter := newTestWorkerStarter()
	starter.startErr = errors.Errorf("test error")
	runner.StartWorker(context.Background(), "id", starter.start)

	// Wait for the runner start waiting for the restart delay.
	select {
	case <-clock.Alarms():
	case <-time.After(longWait):
		c.Fatalf("runner never slept")
	}

	errc := make(chan error, 1)
	stop := make(chan struct{})
	go func() {
		w, err := runner.Worker("id", stop)
		c.Check(w, gc.Equals, nil)
		errc <- err
	}()
	select {
	case err := <-errc:
		c.Fatalf("got unexpected result, error %q", err)
	case <-time.After(shortWait):
	}

	close(stop)
	select {
	case err := <-errc:
		c.Assert(errors.Is(err, worker.ErrAborted), jc.IsTrue)
	case <-time.After(longWait):
		c.Fatalf("timed out waiting for worker")
	}
}

func (*RunnerSuite) TestWorkerConcurrent(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: time.Microsecond,
	})
	defer worker.Stop(runner)
	const concurrency = 5
	workers := make([]worker.Worker, concurrency)
	for i := range workers {
		w := &errorWorker{
			err: errors.Errorf("worker %d error", i),
		}
		workers[i] = w
		err := runner.StartWorker(context.Background(), fmt.Sprint("id", i), func(context.Context) (worker.Worker, error) {
			return w, nil
		})
		c.Assert(err, gc.Equals, nil)
	}
	var count int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Start some concurrent fetchers.
	for i := range workers {
		i := i
		id := fmt.Sprint("id", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				atomic.AddInt64(&count, 1)
				w, err := runner.Worker(id, stop)
				select {
				case <-stop:
					return
				default:
				}
				if !c.Check(w, gc.Equals, workers[i], gc.Commentf("worker %d", i)) {
					return
				}
				c.Check(err, gc.Equals, nil)
			}
		}()
	}
	c.Logf("sleeping")
	time.Sleep(50 * time.Millisecond)
	c.Logf("closing stop channel")
	close(stop)
	wg.Wait()
	c.Logf("count %d", count)
	if count < 5 {
		c.Errorf("not enough iterations - maybe it's not working (got %d, need 5)", count)
	}
}

func (*RunnerSuite) TestWorkerWhenRunnerKilledWhileWaiting(c *gc.C) {
	t0 := time.Now()
	clock := testclock.NewClock(t0)
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: time.Second,
		Clock:        clock,
	})
	defer worker.Stop(runner)
	starter := newTestWorkerStarter()
	starter.startErr = errors.Errorf("test error")
	runner.StartWorker(context.Background(), "id", starter.start)

	// Wait for the runner start waiting for the restart delay.
	select {
	case <-clock.Alarms():
	case <-time.After(longWait):
		c.Fatalf("runner never slept")
	}

	errc := make(chan error, 1)
	go func() {
		w, err := runner.Worker("id", nil)
		c.Check(w, gc.Equals, nil)
		errc <- err
	}()
	runner.Kill()
	select {
	case err := <-errc:
		c.Assert(errors.Is(err, worker.ErrDead), jc.IsTrue)
	case <-time.After(longWait):
		c.Fatalf("timed out waiting for worker")
	}
}

func (*RunnerSuite) TestWorkerWhenWorkerRemovedWhileWaiting(c *gc.C) {
	t0 := time.Now()
	clock := testclock.NewClock(t0)
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: time.Second,
		Clock:        clock,
	})
	defer worker.Stop(runner)
	starter := newTestWorkerStarter()
	starter.startErr = errors.Errorf("test error")
	runner.StartWorker(context.Background(), "id", starter.start)

	// Wait for the runner start waiting for the restart delay.
	select {
	case <-clock.Alarms():
	case <-time.After(longWait):
		c.Fatalf("runner never slept")
	}

	errc := make(chan error, 1)
	go func() {
		w, err := runner.Worker("id", nil)
		c.Check(w, gc.Equals, nil)
		errc <- err
	}()
	// Wait a little bit until we're pretty sure that the Worker
	// call will be waiting.
	time.Sleep(10 * time.Millisecond)
	err := runner.StopWorker("id")
	c.Assert(err, gc.Equals, nil)

	// Wait until the runner gets around to restarting
	// the worker.
	// TODO(rog) this shouldn't be necessary - the Worker
	// call should really fail immediately we remove the
	// worker, but that requires some additional mechanism
	// to interrupt the runWorker goroutine while it's sleeping,
	// which doesn't exist yet.
	clock.Advance(time.Second)
	select {
	case err := <-errc:
		c.Assert(err, jc.Satisfies, errors.IsNotFound)
	case <-time.After(longWait):
		c.Fatalf("timed out waiting for worker")
	}
}

func (*RunnerSuite) TestWorkerWhenStartCallsGoexit(c *gc.C) {
	runner := worker.NewRunner(worker.RunnerParams{
		IsFatal: allFatal,
	})
	defer worker.Stop(runner)

	runner.StartWorker(context.Background(), "id", func(context.Context) (worker.Worker, error) {
		runtime.Goexit()
		panic("unreachable")
	})
	c.Assert(runner.Wait(), gc.ErrorMatches, `runtime.Goexit called in running worker - probably inappropriate Assert`)
}

func (*RunnerSuite) TestRunnerReport(c *gc.C) {
	// Use a non UTC timezone to show times output in UTC.
	// Vostok is +6 for the entire year.
	loc, err := time.LoadLocation("Antarctica/Vostok")
	c.Assert(err, jc.ErrorIsNil)
	t0 := time.Date(2018, 8, 7, 19, 15, 42, 0, loc)
	started := make(chan worker.Worker)
	clock := testclock.NewClock(t0)
	runner := worker.NewRunnerWithNotify(worker.RunnerParams{
		IsFatal:      noneFatal,
		RestartDelay: time.Second,
		Clock:        clock,
	}, started)
	defer worker.Stop(runner)

	for i := 0; i < 5; i++ {
		var report map[string]interface{}
		// Only have reports for half of them.
		if i%2 == 0 {
			report = map[string]interface{}{"index": i}
		}
		starter := newTestWorkerStarterWithReport(report)
		runner.StartWorker(context.Background(), fmt.Sprintf("worker-%d", i), starter.start)
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			c.Fatalf("worker %d failed to start", i)
		}
	}

	report := runner.Report()
	c.Assert(report, jc.DeepEquals, map[string]interface{}{
		"workers": map[string]interface{}{
			"worker-0": map[string]interface{}{
				"report": map[string]interface{}{
					"index": 0},
				"state":   "started",
				"started": "2018-08-07 13:15:42",
			},
			"worker-1": map[string]interface{}{
				"state":   "started",
				"started": "2018-08-07 13:15:42",
			},
			"worker-2": map[string]interface{}{
				"report": map[string]interface{}{
					"index": 2},
				"state":   "started",
				"started": "2018-08-07 13:15:42",
			},
			"worker-3": map[string]interface{}{
				"state":   "started",
				"started": "2018-08-07 13:15:42",
			},
			"worker-4": map[string]interface{}{
				"report": map[string]interface{}{
					"index": 4},
				"state":   "started",
				"started": "2018-08-07 13:15:42",
			},
		}})
}

type testWorkerStarter struct {
	startCount int32

	// startNotify receives true when the worker starts
	// and false when it exits. If startErr is non-nil,
	// it sends false only.
	startNotify chan bool

	// If stopWait is non-nil, the worker will
	// wait for a value to be sent on it before
	// exiting.
	stopWait chan struct{}

	// Sending a value on die causes the worker
	// to die with the given error.
	die chan error

	// If startErr is non-nil, the worker will die immediately
	// with this error after starting.
	startErr error

	// If stopErr is non-nil, the worker will die with this
	// error when asked to stop.
	stopErr error

	// If killErr is non-nil, the worker will die with this
	// error when killed.
	killErr error

	// The hook function is called after starting the worker.
	hook func()

	// Any report values are returned in the Report call.
	report map[string]interface{}
}

func newTestWorkerStarter() *testWorkerStarter {
	return &testWorkerStarter{
		die:         make(chan error, 1),
		startNotify: make(chan bool, 100),
		hook:        func() {},
	}
}

func newTestWorkerStarterWithReport(report map[string]interface{}) *testWorkerStarter {
	return &testWorkerStarter{
		die:         make(chan error, 1),
		startNotify: make(chan bool, 100),
		report:      report,
		hook:        func() {},
	}
}

func (starter *testWorkerStarter) assertStarted(c *gc.C, started bool) {
	select {
	case isStarted := <-starter.startNotify:
		c.Assert(isStarted, gc.Equals, started)
	case <-time.After(longWait):
		c.Fatalf("timed out waiting for start notification")
	}
}

func (starter *testWorkerStarter) assertNeverStarted(c *gc.C, restartDelay time.Duration) {
	select {
	case isStarted := <-starter.startNotify:
		c.Fatalf("got unexpected start notification: %v", isStarted)
	case <-time.After(restartDelay + shortWait):
	}
}

func (starter *testWorkerStarter) start(ctx context.Context) (worker.Worker, error) {
	if count := atomic.AddInt32(&starter.startCount, 1); count != 1 {
		panic(fmt.Errorf("unexpected start count %d; expected 1", count))
	}
	if starter.startErr != nil {
		starter.startNotify <- false
		if count := atomic.AddInt32(&starter.startCount, -1); count != 0 {
			panic(fmt.Errorf("unexpected start count %d; expected 0", count))
		}
		return nil, starter.startErr
	}
	task := &testWorker{
		starter: starter,
		err:     starter.killErr,
	}
	starter.startNotify <- true
	task.tomb.Go(task.run)
	return task, nil
}

type testWorker struct {
	starter *testWorkerStarter
	err     error
	tomb    tomb.Tomb
}

func (t *testWorker) Kill() {
	t.tomb.Kill(t.err)
}

func (t *testWorker) Wait() error {
	return t.tomb.Wait()
}

func (t *testWorker) Report() map[string]interface{} {
	return t.starter.report
}

func (t *testWorker) run() (err error) {
	t.starter.hook()
	select {
	case <-t.tomb.Dying():
		err = t.starter.stopErr // tomb.ErrDying
	case err = <-t.starter.die:
	}
	if t.starter.stopWait != nil {
		<-t.starter.stopWait
	}
	t.starter.startNotify <- false
	if count := atomic.AddInt32(&t.starter.startCount, -1); count != 0 {
		panic(fmt.Errorf("unexpected start count %d; expected 0", count))
	}
	return err
}

// errorWorker holds a worker that immediately dies with an error.
type errorWorker struct {
	err error
}

func (w errorWorker) Kill() {
}

func (w errorWorker) Wait() error {
	return w.err
}

func noneFatal(error) bool {
	return false
}

func allFatal(error) bool {
	return true
}

// containsChecker checks that a slice of strings contains a given string.
type containsChecker struct {
	*gc.CheckerInfo
}

// Contains checks that a slice of strings contains a given string.
var Contains gc.Checker = &containsChecker{
	CheckerInfo: &gc.CheckerInfo{Name: "Contains", Params: []string{"obtained", "expected"}},
}

func (checker *containsChecker) Check(params []interface{}, names []string) (bool, string) {
	expected, ok := params[1].(string)
	if !ok {
		return false, "expected must be a string"
	}

	obtained, isSlice := params[0].([]string)
	if isSlice {
		for _, s := range obtained {
			if s == expected {
				return true, ""
			}
		}
		return false, ""
	}

	return false, "Obtained value is not a []string"
}
