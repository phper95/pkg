package errors

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"runtime"
	"strings"
)

var (
	dunno     = []byte("???")
	centerDot = []byte("·")
	dot       = []byte(".")
)

type StackError interface {
	Error() string
	StackTrace() string
}

type stackError struct {
	err        error
	stackTrace string
}

func (e stackError) Error() string {
	return fmt.Sprintf("%v\n%v", e.err, e.stackTrace)
}

func (e stackError) StackTrace() string {
	return e.stackTrace
}

func StackErrorf(msg string, args ...interface{}) error {
	stack := ""
	// See if any arg is already embedding a stack - no need to
	// recompute something expensive and make the message unreadable.
	for _, arg := range args {
		if stackErr, ok := arg.(stackError); ok {
			stack = stackErr.stackTrace
			break
		}
	}

	if stack == "" {
		// magic 5 trims off just enough stack data to be clear
		stack = string(Stack(5))
	}

	return stackError{fmt.Errorf(msg, args...), stack}
}

func StackWithoutLF(calldepth int) string {
	traceback := string(Stack(calldepth))
	return strings.Replace(
		strings.Replace(traceback, "\n\t", "[", -1),
		"\n", "]|", -1,
	)
}

// Taken from runtime/debug.go
func Stack(calldepth int) []byte {
	return stack(calldepth)
}

func stack(calldepth int) []byte {
	buf := new(bytes.Buffer)
	var lines [][]byte
	var lastFile string
	for i := calldepth; ; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}

		fmt.Fprintf(buf, "%s:%d (0x%x)\n", file, line, pc)
		if file != lastFile {
			data, err := ioutil.ReadFile(file)
			if err != nil {
				continue
			}
			lines = bytes.Split(data, []byte{'\n'})
			lastFile = file
		}
		line--
		fmt.Fprintf(buf, "\t%s: %s\n", function(pc), source(lines, line))
	}
	return buf.Bytes()
}

func source(lines [][]byte, n int) []byte {
	if n < 0 || n >= len(lines) {
		return dunno
	}
	return bytes.Trim(lines[n], " \t")
}

func function(pc uintptr) []byte {
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return dunno
	}
	name := []byte(fn.Name())
	if period := bytes.Index(name, dot); period >= 0 {
		name = name[period+1:]
	}
	name = bytes.Replace(name, centerDot, dot, -1)
	return name
}
