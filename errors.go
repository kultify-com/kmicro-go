package kmicro

import "errors"

const (
	ErrorCodeInternal  = "500"
	ErrorCodeForbidden = "403"
	ErrorCodeNotFound  = "404"
)

// CodedError is a code THIS handler chose. Only this type decides the code its
// endpoint answers with.
type CodedError struct {
	Code    string
	Wrapped error
}

func (e *CodedError) Error() string {
	if e.Wrapped == nil {
		return ""
	}
	return e.Wrapped.Error()
}

func (e *CodedError) Unwrap() error { return e.Wrapped }

// CallError is a code another endpoint answered with. It is deliberately a
// different type: a handler that passes a downstream failure on must not
// republish the downstream module's code as its own answer, and the estate's
// one authenticating boundary wraps exactly such an error around its refusal.
type CallError struct {
	Code    string
	Wrapped error
}

func (e *CallError) Error() string {
	if e.Wrapped == nil {
		return ""
	}
	return e.Wrapped.Error()
}

func (e *CallError) Unwrap() error { return e.Wrapped }

func WithCode(code string, err error) error {
	if err == nil {
		return nil
	}
	return &CodedError{Code: code, Wrapped: err}
}

// ErrorCode reports the code a failure carries, whether this process chose it
// or a call answered with it. Callers wrap, so it looks through the chain.
func ErrorCode(err error) (string, bool) {
	var coded *CodedError
	if errors.As(err, &coded) {
		return coded.Code, true
	}
	var called *CallError
	if errors.As(err, &called) {
		return called.Code, true
	}
	return "", false
}

// replyCode is what an endpoint answers with. It reads only what this handler
// chose, and never an empty code: Call decides a reply is an error by that
// header being non-empty, so an empty one reads as success.
func replyCode(err error, fallback string) string {
	var coded *CodedError
	if errors.As(err, &coded) && coded.Code != "" {
		return coded.Code
	}
	return fallback
}
