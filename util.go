package mongonet

import (
	"fmt"
	"runtime"
	"strings"
)

// ---

func MergeErrors(errors ...error) error {
	n, laste := 0, error(nil)

	for _, e := range errors {
		if e != nil {
			n++
			laste = e
		}
	}
	switch n {
	case 0:
		return nil
	case 1:
		return laste
	default:
		s := make([]string, 0, n)
		for _, e := range errors {
			if e != error(nil) {
				s = append(s, e.Error())
			}
		}
		return fmt.Errorf("Multiple errors: %v", strings.Join(s, "; "))
	}
}

type ProxyRetryError struct {
	MsgToRetry     Message
	PreviousResult SimpleBSON
	RetryOnRs      string
	RetryCount     int
}

func (e *ProxyRetryError) Error() string {
	if e.RetryOnRs == "" {
		return fmt.Sprintf("ProxyRetryError - going to retry on local replica set, %v times remaining", e.RetryCount)
	}
	return fmt.Sprintf("ProxyRetryError - going to retry on %s, %v times remaining", e.RetryOnRs, e.RetryCount)
}

func NewProxyRetryError(msgToRetry Message, previousRes SimpleBSON, retryOnRs string) *ProxyRetryError {
	return &ProxyRetryError{
		msgToRetry,
		previousRes,
		retryOnRs,
		1,
	}
}

// NewProxyRetryErrorWithRetryCount returns a ProxyRetryError with retryCount
// number of retries. This is the same as NewProxyRetryError with retryCount
// set to 1. If the retry fails (non-zero errorCode) retryCount number of times
// we will return the error back to the proxy.
func NewProxyRetryErrorWithRetryCount(msgToRetry Message, previousRes SimpleBSON, retryOnRs string, retryCount int) *ProxyRetryError {
	return &ProxyRetryError{
		msgToRetry,
		previousRes,
		retryOnRs,
		retryCount,
	}
}

type StackError struct {
	Message    string
	Stacktrace []string
}

func NewStackErrorf(messageFmt string, args ...interface{}) *StackError {
	return &StackError{
		Message:    fmt.Sprintf(messageFmt, args...),
		Stacktrace: stacktrace(),
	}
}

func (self *StackError) Error() string {
	return fmt.Sprintf("%s\n\t%s", self.Message, strings.Join(self.Stacktrace, "\n\t"))
}

func stacktrace() []string {
	ret := make([]string, 0, 2)
	for skip := 2; true; skip++ {
		_, file, line, ok := runtime.Caller(skip)
		if ok == false {
			break
		}

		ret = append(ret, fmt.Sprintf("at %s:%d", stripDirectories(file, 2), line))
	}

	return ret
}

func stripDirectories(filepath string, toKeep int) string {
	var idxCutoff int
	if idxCutoff = strings.LastIndex(filepath, "/"); idxCutoff == -1 {
		return filepath
	}

	for dirToKeep := 0; dirToKeep < toKeep; dirToKeep++ {
		switch idx := strings.LastIndex(filepath[:idxCutoff], "/"); idx {
		case -1:
			break
		default:
			idxCutoff = idx
		}
	}

	return filepath[idxCutoff+1:]
}
