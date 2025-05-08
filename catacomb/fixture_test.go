// Copyright 2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package catacomb_test

import (
	"time"

	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/tomb.v2"

	"github.com/juju/worker/v4"
	"github.com/juju/worker/v4/catacomb"
)

type cleaner interface {
	AddCleanup(func(*gc.C))
}

type fixture struct {
	catacomb catacomb.Catacomb
	cleaner  cleaner
}

func (fix *fixture) run(c *gc.C, task func(), init ...worker.Worker) error {
	err := catacomb.Invoke(catacomb.Plan{
		Site: &fix.catacomb,
		Work: func() error { task(); return nil },
		Init: init,
	})
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-fix.catacomb.Dead():
	case <-time.After(testing.LongWait):
		c.Fatalf("timed out")
	}
	return fix.catacomb.Wait()
}

func (fix *fixture) waitDying(c *gc.C) {
	select {
	case <-fix.catacomb.Dying():
	case <-time.After(testing.LongWait):
		c.Fatalf("timed out; still alive")
	}
}

func (fix *fixture) assertDying(c *gc.C) {
	select {
	case <-fix.catacomb.Dying():
	default:
		c.Fatalf("still alive")
	}
}

func (fix *fixture) assertNotDying(c *gc.C) {
	select {
	case <-fix.catacomb.Dying():
		c.Fatalf("already dying")
	default:
	}
}

func (fix *fixture) assertDead(c *gc.C) {
	select {
	case <-fix.catacomb.Dead():
	default:
		c.Fatalf("not dead")
	}
}

func (fix *fixture) assertNotDead(c *gc.C) {
	select {
	case <-fix.catacomb.Dead():
		c.Fatalf("already dead")
	default:
	}
}

func (fix *fixture) assertAddAlive(c *gc.C, w *errorWorker) {
	err := fix.catacomb.Add(w)
	c.Assert(err, jc.ErrorIsNil)
	w.waitStillAlive(c)
}

func (fix *fixture) startErrorWorker(c *gc.C, err error) *errorWorker {
	ew := &errorWorker{}
	ew.tomb.Go(func() error {
		defer ew.tomb.Kill(err)
		<-ew.tomb.Dying()
		return nil
	})
	fix.cleaner.AddCleanup(func(_ *gc.C) {
		ew.stop()
	})
	return ew
}

type errorWorker struct {
	tomb tomb.Tomb
}

func (ew *errorWorker) Kill() {
	ew.tomb.Kill(nil)
}

func (ew *errorWorker) Wait() error {
	return ew.tomb.Wait()
}

func (ew *errorWorker) stop() {
	ew.Kill()
	ew.Wait()
}

func (ew *errorWorker) waitStillAlive(c *gc.C) {
	select {
	case <-ew.tomb.Dying():
		c.Fatalf("already dying")
	case <-time.After(testing.ShortWait):
	}
}

func (ew *errorWorker) assertDead(c *gc.C) {
	select {
	case <-ew.tomb.Dead():
	default:
		c.Fatalf("not yet dead")
	}
}
