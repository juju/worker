// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package worker

// Reporter defines an interface for extracting human-relevant information
// from a worker.
type Reporter interface {

	// Report returns a map describing the state of the receiver. It is expected
	// to be goroutine-safe.
	//
	// It is polite and helpful to use the Key* constants and conventions defined
	// and described in this package, where appropriate, but that's for the
	// convenience of the humans that read the reports; we don't and shouldn't
	// have any code that depends on particular Report formats.
	Report() map[string]interface{}
}

// The Key constants describe the constant features of an Engine's Report.
const (

	// KeyState applies to a worker; possible values are "starting", "started",
	// "stopping", or "stopped". Or it might be something else, in distant
	// Reporter implementations; don't make assumptions.
	KeyState = "state"

	// KeyReport holds an arbitrary map of information returned by a manifold
	// Worker that is also a Reporter.
	KeyReport = "report"

	// KeyLastStart holds the time of when the worker was last started.
	KeyLastStart = "started"
)
