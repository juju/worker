// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package worker

import (
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
)

// NewRunnerWithNotify starts the runner and sets the started notification channel.
func NewRunnerWithNotify(c *gc.C, p RunnerParams, notify chan<- Worker) *Runner {
	r, err := NewRunner(p)
	c.Assert(err, jc.ErrorIsNil)
	r.notifyStarted = notify
	return r
}
