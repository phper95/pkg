module pkg/redis

replace (
	pkg/errors => ../errors
	pkg/timeutil => ../timeutil
	pkg/trace => ../trace
)

go 1.16

require (
	github.com/go-redis/redis/v7 v7.4.1
	pkg/errors v0.0.0-00010101000000-000000000000
	pkg/timeutil v0.0.0-00010101000000-000000000000
	pkg/trace v0.0.0-00010101000000-000000000000
)
