// Copyright 2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package workertest_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/juju/testing"
	gc "gopkg.in/check.v1"

	"github.com/juju/worker/v4/workertest"
)

type Suite struct {
	testing.IsolationSuite
}

var _ = gc.Suite(&Suite{})

func (s *Suite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.PatchValue(workertest.KillTimeout, time.Second)
}

func (s *Suite) TestCheckAliveSuccess(c *gc.C) {
	w := workertest.NewErrorWorker(nil)
	defer workertest.CleanKill(c, w)

	workertest.CheckAlive(c, w)
}

func (s *Suite) TestCheckAliveFailure(c *gc.C) {
	w := workertest.NewDeadWorker(nil)

	x := &noFail{C: c}
	workertest.CheckAlive(x, w)
	c.Assert(x.failed.Load(), gc.Equals, true)
}

func (s *Suite) TestCheckKilledSuccess(c *gc.C) {
	expect := errors.New("snifplog")
	w := workertest.NewErrorWorker(expect)
	defer workertest.DirtyKill(c, w)

	w.Kill()
	err := workertest.CheckKilled(c, w)
	c.Check(err, gc.Equals, expect)
}

func (s *Suite) TestCheckKilledTimeout(c *gc.C) {
	w := workertest.NewErrorWorker(nil)
	defer workertest.CleanKill(c, w)

	ctx, cancel := context.WithTimeout(c.Context(), 10*time.Second)
	defer cancel()
	x := &noFail{C: c, ctx: ctx}
	err := workertest.CheckKilled(x, w)
	c.Assert(x.failed.Load(), gc.Equals, true)
	c.Check(err, gc.ErrorMatches, "workertest: worker not stopping")
}

func (s *Suite) TestCheckKillSuccess(c *gc.C) {
	expect := errors.New("fledbon")
	w := workertest.NewErrorWorker(expect)
	defer workertest.DirtyKill(c, w)

	err := workertest.CheckKill(c, w)
	c.Check(err, gc.Equals, expect)
}

func (s *Suite) TestCheckKillTimeout(c *gc.C) {
	w := workertest.NewForeverWorker(nil)
	defer w.ReallyKill()

	ctx, cancel := context.WithTimeout(c.Context(), 10*time.Second)
	defer cancel()
	x := &noFail{C: c, ctx: ctx}
	err := workertest.CheckKill(x, w)
	c.Assert(x.failed.Load(), gc.Equals, true)
	c.Check(err, gc.ErrorMatches, "workertest: worker not stopping")
}

func (s *Suite) TestCleanKillSuccess(c *gc.C) {
	w := workertest.NewErrorWorker(nil)

	workertest.CleanKill(c, w)
}

func (s *Suite) TestCleanKillFailure(c *gc.C) {
	w := workertest.NewErrorWorker(errors.New("kebdrix"))

	x := &noFail{C: c}
	workertest.CleanKill(x, w)
	c.Assert(x.failed.Load(), gc.Equals, true)
}

func (s *Suite) TestCleanKillTimeout(c *gc.C) {
	w := workertest.NewForeverWorker(nil)
	defer w.ReallyKill()

	ctx, cancel := context.WithTimeout(c.Context(), 10*time.Second)
	defer cancel()
	x := &noFail{C: c, ctx: ctx}
	workertest.CleanKill(x, w)
	c.Assert(x.failed.Load(), gc.Equals, true)
}

func (s *Suite) TestDirtyKillSuccess(c *gc.C) {
	w := workertest.NewErrorWorker(errors.New("hifstit"))

	workertest.DirtyKill(c, w)
}

func (s *Suite) TestDirtyKillTimeout(c *gc.C) {
	w := workertest.NewForeverWorker(nil)
	defer w.ReallyKill()

	ctx, cancel := context.WithTimeout(c.Context(), 10*time.Second)
	defer cancel()
	x := &noFail{C: c, ctx: ctx}
	workertest.DirtyKill(x, w)
	c.Assert(x.failed.Load(), gc.Equals, true)
}

// noFail is a helper to check failed checkers
type noFail struct {
	*gc.C
	failed atomic.Bool
	ctx    context.Context
}

func (c *noFail) Context() context.Context {
	if c.ctx == nil {
		return c.C.Context()
	}
	return c.ctx
}

func (c *noFail) Error(args ...any) {
	c.Logf("%s", fmt.Sprintln(args...))
	c.failed.Store(true)
}

func (c *noFail) Errorf(format string, args ...any) {
	c.Logf(format, args...)
	c.failed.Store(true)
}

func (c *noFail) Fatal(args ...any) {
	c.Logf("%s", fmt.Sprintln(args...))
	c.failed.Store(true)
	panic("failed")
}

func (c *noFail) Fatalf(format string, args ...any) {
	c.Logf(format, args...)
	c.failed.Store(true)
	panic("failed")
}

func (c *noFail) Check(obtained any, checker gc.Checker, args ...any) bool {
	ok, errString := checker.Check(append([]any{obtained}, args...), checker.Info().Params)
	if ok {
		return true
	}
	c.Logf("%s", errString)
	c.failed.Store(true)
	return false
}

func (c *noFail) Assert(obtained any, checker gc.Checker, args ...any) {
	ok, errString := checker.Check(append([]any{obtained}, args...), checker.Info().Params)
	if ok {
		return
	}
	c.Logf("%s", errString)
	c.failed.Store(true)
	panic("failed")
}
