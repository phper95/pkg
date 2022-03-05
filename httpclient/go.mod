module pkg/httpclient

go 1.16

replace (
	pkg/errors => ../errors
	pkg/timeutil => ../timeutil
	pkg/trace => ../trace
)

require (
	go.uber.org/zap v1.21.0
	pkg/errors v0.0.0-00010101000000-000000000000
	pkg/trace v0.0.0-00010101000000-000000000000
)
