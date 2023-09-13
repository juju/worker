// Copyright 2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package dependency

import (
	"context"
	"fmt"

	"github.com/juju/errors"

	"github.com/juju/worker/v3"
)

// snapshot encapsulates a snapshot of workers and output funcs and implements
// Container.
type snapshot struct {
	// ctx represents the context in which the snapshot was taken.
	ctx context.Context

	// clientName is the name of the manifold for whose convenience this exists.
	clientName string

	// expired is closed when the scope should no longer be used.
	expired chan struct{}

	// workers holds the snapshot of manifold workers.
	workers map[string]worker.Worker

	// outputs holds the snapshot of manifold output funcs.
	outputs map[string]OutputFunc

	// accessLog holds the names and types of resource requests, and any error
	// encountered. It does not include requests made after expiry.
	accessLog []resourceAccess

	// logger is used to pass the logger to the workers.
	logger Logger
}

// Get is part of the Context interface.
func (s *snapshot) Get(resourceName string, out interface{}) error {
	s.logger.Tracef("%q manifold requested %q resource", s.clientName, resourceName)
	select {
	case <-s.expired:
		return errors.New("expired context: cannot be used outside Start func")
	default:
		err := s.rawAccess(resourceName, out)
		s.accessLog = append(s.accessLog, resourceAccess{
			name: resourceName,
			as:   fmt.Sprintf("%T", out),
			err:  err,
		})
		return err
	}
}

// expire closes the expired channel. Calling it more than once will panic.
func (s *snapshot) expire() {
	close(s.expired)
}

// rawAccess is a GetResourceFunc that neither checks enpiry nor records access.
func (s *snapshot) rawAccess(resourceName string, out interface{}) error {
	input, found := s.workers[resourceName]
	if !found {
		return errors.Annotatef(ErrMissing, "%q not declared", resourceName)
	} else if input == nil {
		return errors.Annotatef(ErrMissing, "%q not running", resourceName)
	}
	if out == nil {
		// No conversion necessary, just an exist check.
		return nil
	}
	convert := s.outputs[resourceName]
	if convert == nil {
		return errors.Annotatef(ErrMissing, "%q not exposed", resourceName)
	}
	return convert(input, out)
}

// resourceAccess describes a call made to (*context).Get.
type resourceAccess struct {

	// name is the name of the resource requested.
	name string

	// as is the string representation of the type of the out param.
	as string

	// err is any error returned from rawAccess.
	err error
}
