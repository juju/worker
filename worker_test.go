// Copyright 2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package worker_test

import (
	"time"

	"github.com/juju/errors"
	"github.com/juju/testing"
	gc "gopkg.in/check.v1"

	"github.com/juju/worker/v2"
)

type WorkerSuite struct {
	testing.IsolationSuite
}

var _ = gc.Suite(&WorkerSuite{})

func (*WorkerSuite) TestStop(c *gc.C) {
	killed := 0
	testErr := errors.New("an error")
	w := hookWorker{
		kill: func() {
			killed++
		},
		wait: func() error {
			c.Check(killed, gc.Equals, 1)
			return testErr
		},
	}
	err := worker.Stop(w)
	c.Assert(killed, gc.Equals, 1)
	c.Assert(err, gc.Equals, testErr)
}

func (*WorkerSuite) TestDead(c *gc.C) {
	waiting := make(chan struct{})
	w := hookWorker{
		wait: func() error {
			waiting <- struct{}{}
			<-waiting
			return nil
		},
	}
	deadCh := worker.Dead(w)
	select {
	case <-waiting:
	case <-time.After(longWait):
		c.Fatalf("wait never called")
	}
	select {
	case <-deadCh:
		c.Fatalf("received value before worker is dead")
	case <-time.After(shortWait):
	}
	close(waiting)
	select {
	case <-deadCh:
	case <-time.After(longWait):
		c.Fatalf("never received on dead channel")
	}
}

func (*WorkerSuite) TestDeadWithDeadMethod(c *gc.C) {
	deadCh := make(chan struct{})
	w := hookWorkerWithDead{
		hookWorker: hookWorker{
			wait: func() error {
				c.Error("wait should not be called")
				return nil
			},
		},
		dead: func() <-chan struct{} {
			return deadCh
		},
	}
	ch := worker.Dead(w)
	c.Assert(ch, gc.Equals, (<-chan struct{})(deadCh))
}

func (*WorkerSuite) TestDeadWithDeadMethodReturningNil(c *gc.C) {
	waitCalls, deadCalls := 0, 0
	w := hookWorkerWithDead{
		hookWorker: hookWorker{
			wait: func() error {
				waitCalls++
				return nil
			},
		},
		dead: func() <-chan struct{} {
			deadCalls++
			return nil
		},
	}
	ch := worker.Dead(w)
	<-ch
	c.Assert(waitCalls, gc.Equals, 1)
	c.Assert(deadCalls, gc.Equals, 1)
}

// hookWorker implements worker.Worker by
// deferring to its member functions.
type hookWorker struct {
	kill func()
	wait func() error
}

func (w hookWorker) Kill() {
	w.kill()
}

func (w hookWorker) Wait() error {
	return w.wait()
}

type hookWorkerWithDead struct {
	hookWorker
	dead func() <-chan struct{}
}

func (w hookWorkerWithDead) Dead() <-chan struct{} {
	return w.dead()
}
