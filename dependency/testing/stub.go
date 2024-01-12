// Copyright 2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package testing

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/juju/errors"

	"github.com/juju/worker/v4/dependency"
)

// NewStubResource creates a single StubResource with the given
// outputs. (If you need to specify an error result, use the
// StubResource type directly.)
func NewStubResource(outputs ...interface{}) StubResource {
	return StubResource{Outputs: outputs}
}

// StubResource is used to define the behaviour of a StubGetResource
// func for a particular resource name.
type StubResource struct {
	Outputs []interface{}
	Error   error
}

// NewStubResources converts raw into a StubResources by assuming that
// any non-error value is intended to be an output. Multiple outputs
// can set by specifying a slice of interface{}s.
func NewStubResources(raw map[string]interface{}) StubResources {
	resources := StubResources{}
	for name, value := range raw {
		var resource StubResource
		switch value := value.(type) {
		case error:
			resource = StubResource{Error: value}
		case []interface{}:
			resource = StubResource{Outputs: value}
		default:
			resource = StubResource{Outputs: []interface{}{value}}
		}
		resources[name] = resource
	}
	return resources
}

// StubResources defines the complete behaviour of a StubGetResource func.
type StubResources map[string]StubResource

// Getter returns a dependency.Getter that never aborts, backed by resources.
func (resources StubResources) Getter() dependency.Getter {
	return &Getter{
		resources: resources,
	}
}

// StubGetter returns a Getter.
func StubGetter(raw map[string]interface{}) *Getter {
	return &Getter{
		resources: NewStubResources(raw),
	}
}

// Getter implements dependency.Getter for convenient testing of
// dependency.StartFuncs.
type Getter struct {
	resources StubResources
}

// Get is part of the dependency.Getter interface.
func (g *Getter) Get(name string, outPtr interface{}) error {
	resource, found := g.resources[name]
	if !found {
		return errors.Errorf("unexpected resource name: %s", name)
	} else if resource.Error != nil {
		return resource.Error
	}
	if outPtr == nil {
		return nil
	}
	outPtrV := reflect.ValueOf(outPtr)
	if outPtrV.Kind() != reflect.Ptr {
		return errors.Errorf("outPtr should be a pointer; is %#v", outPtr)
	}
	outV := outPtrV.Elem()
	outT := outV.Type()

	var errorOutputs []string
	for _, output := range resource.Outputs {
		setV := reflect.ValueOf(output)
		setT := setV.Type()
		if setT.ConvertibleTo(outT) {
			outV.Set(setV.Convert(outT))
			return nil
		}
		errorOutputs = append(errorOutputs, fmt.Sprintf("%#v", output))
	}
	return errors.Errorf("cannot set %s into %T", strings.Join(errorOutputs, ", "), outPtr)
}
