// Copyright 2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package dependency_test

import (
	"time"

	"github.com/juju/clock/testclock"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"github.com/juju/worker/v4/dependency"
	"github.com/juju/worker/v4/workertest"
)

type ReportSuite struct {
	testing.IsolationSuite
	fix   *engineFixture
	clock *testclock.Clock
}

var _ = gc.Suite(&ReportSuite{})

func (s *ReportSuite) SetUpTest(c *gc.C) {
	// Use a non UTC timezone to show times output in UTC.
	// Vostok is +6 for the entire year.
	loc, err := time.LoadLocation("Antarctica/Vostok")
	c.Assert(err, jc.ErrorIsNil)
	t0 := time.Date(2018, 8, 7, 19, 15, 42, 0, loc)
	s.IsolationSuite.SetUpTest(c)
	s.clock = testclock.NewClock(t0)
	s.fix = &engineFixture{clock: s.clock}
}

func (s *ReportSuite) TestReportStarted(c *gc.C) {
	s.fix.run(c, func(engine *dependency.Engine) {
		report := engine.Report()
		c.Check(report, jc.DeepEquals, map[string]interface{}{
			"state":     "started",
			"manifolds": map[string]interface{}{},
		})
	})
}

func (s *ReportSuite) TestReportStopped(c *gc.C) {
	s.fix.run(c, func(engine *dependency.Engine) {
		workertest.CleanKill(c, engine)
		report := engine.Report()
		c.Check(report, jc.DeepEquals, map[string]interface{}{
			"state":     "stopped",
			"manifolds": map[string]interface{}{},
		})
	})
}

func (s *ReportSuite) TestReportStopping(c *gc.C) {
	s.fix.run(c, func(engine *dependency.Engine) {
		mh1 := newErrorIgnoringManifoldHarness()
		err := engine.Install("task", mh1.Manifold())
		c.Assert(err, jc.ErrorIsNil)
		defer func() {
			engine.Kill()
			// we *must* inject this between Kill and Wait
			mh1.InjectError(c, nil)
			err := engine.Wait()
			c.Check(err, jc.ErrorIsNil)
		}()
		mh1.AssertOneStart(c)

		// It may take a short time for the main loop to notice
		// the change and stop the "task" worker.
		engine.Kill()
		isTaskStopping := func(report map[string]interface{}) bool {
			manifolds := report["manifolds"].(map[string]interface{})
			task := manifolds["task"].(map[string]interface{})
			switch taskState := task["state"]; taskState {
			case "started":
				return false
			case "stopping":
				return true
			default:
				c.Fatalf("unexpected task state: %v", taskState)
			}
			panic("unreachable")
		}

		var report map[string]interface{}
		for i := 0; i < 3; i++ {
			report = engine.Report()
			if isTaskStopping(report) {
				break
			}
			time.Sleep(testing.ShortWait)
		}
		c.Check(report, jc.DeepEquals, map[string]interface{}{
			"state": "stopping",
			"manifolds": map[string]interface{}{
				"task": map[string]interface{}{
					"state":       "stopping",
					"start-count": 1,
					"started":     "2018-08-07 13:15:42",
					"inputs":      ([]string)(nil),
					"report": map[string]interface{}{
						"key1": "hello there",
					},
				},
			},
		})
	})
}

func (s *ReportSuite) TestReportInputs(c *gc.C) {
	s.fix.run(c, func(engine *dependency.Engine) {
		mh1 := newManifoldHarness()
		err := engine.Install("task", mh1.Manifold())
		c.Assert(err, jc.ErrorIsNil)
		mh1.AssertOneStart(c)

		mh2 := newManifoldHarness("task")
		err = engine.Install("another task", mh2.Manifold())
		c.Assert(err, jc.ErrorIsNil)
		mh2.AssertOneStart(c)

		report := engine.Report()
		c.Check(report, jc.DeepEquals, map[string]interface{}{
			"state": "started",
			"manifolds": map[string]interface{}{
				"task": map[string]interface{}{
					"state":       "started",
					"start-count": 1,
					"started":     "2018-08-07 13:15:42",
					"inputs":      ([]string)(nil),
					"report": map[string]interface{}{
						"key1": "hello there",
					},
				},
				"another task": map[string]interface{}{
					"state":       "started",
					"start-count": 1,
					"started":     "2018-08-07 13:15:42",
					"inputs":      []string{"task"},
					"report": map[string]interface{}{
						"key1": "hello there",
					},
				},
			},
		})
	})
}

func (s *ReportSuite) TestReportError(c *gc.C) {
	s.fix.run(c, func(engine *dependency.Engine) {
		mh1 := newManifoldHarness("missing")
		manifold := mh1.Manifold()
		err := engine.Install("task", manifold)
		c.Assert(err, jc.ErrorIsNil)
		mh1.AssertNoStart(c)

		workertest.CleanKill(c, engine)
		report := engine.Report()
		c.Check(report, jc.DeepEquals, map[string]interface{}{
			"state": "stopped",
			"manifolds": map[string]interface{}{
				"task": map[string]interface{}{
					"state":  "stopped",
					"error":  `"missing" not running: dependency not available`,
					"inputs": []string{"missing"},
				},
			},
		})
	})
}
