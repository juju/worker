// Copyright 2021 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package dependency

// Metrics defines a type for recording the worker life cycle in the dependency
// engine.
type Metrics interface {
	RecordStart(name string)
}

// noopMetrics gives a metric that doesn't do anything.
type noopMetric struct{}

func (noopMetric) RecordStart(name string) {}

// DefaultMetrics returns a metrics implementation that performs no operations,
// but can be used for scenarios where metrics output isn't required.
func DefaultMetrics() Metrics {
	return noopMetric{}
}
