package kmicro

import "errors"

// The codes kmicro itself answers with. The vocabulary is open -- any service
// may answer any string -- and these are the values the estate already sends.
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

// WithCode declares the code this handler's endpoint answers with. It passes a
// nil error through, and an empty code through uncoded: Call decides a reply is
// an error by that header being non-empty, so an empty one would read as
// success.
func WithCode(code string, err error) error {
	if err == nil || code == "" {
		return err
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

// HasCode reports whether a failure carries exactly this code. Prefer it to
// comparing what ErrorCode returns: an uncoded error answers the empty string,
// so the second return is never the term that decides.
func HasCode(err error, code string) bool {
	actual, ok := ErrorCode(err)
	return ok && actual == code
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
