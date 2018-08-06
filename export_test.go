// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package worker

// NewRunnerWithNotify starts the runner and sets the started notification channel.
func NewRunnerWithNotify(p RunnerParams, notify chan<- Worker) *Runner {
	r := NewRunner(p)
	r.notifyStarted = notify
	return r
}
