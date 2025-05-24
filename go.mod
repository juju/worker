module github.com/juju/worker/v4

go 1.24.2

toolchain go1.24.3

require (
	github.com/juju/clock v1.0.3
	github.com/juju/collections v0.0.0-20220203020748-febd7cad8a7a
	github.com/juju/errors v1.0.0
	github.com/juju/loggo v1.0.0
	github.com/juju/testing v1.2.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637
)

require (
	github.com/juju/loggo/v2 v2.1.0 // indirect
	github.com/juju/tc v0.0.0-20250523041547-7f66c35f9f03 // indirect
	github.com/juju/utils/v3 v3.1.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	golang.org/x/crypto v0.28.0 // indirect
	golang.org/x/net v0.30.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace gopkg.in/check.v1 => github.com/hpidcock/gc-compat-tc v0.0.0-20250523041742-c3a83c867edf
