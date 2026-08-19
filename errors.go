package kmicro

import "errors"

const (
	ErrorCodeInternal  = "500"
	ErrorCodeForbidden = "403"
	ErrorCodeNotFound  = "404"
)

type CodedError struct {
	Code    string
	Wrapped error
}

func (e *CodedError) Error() string { return e.Wrapped.Error() }

func (e *CodedError) Unwrap() error { return e.Wrapped }

func WithCode(code string, err error) error {
	if err == nil {
		return nil
	}
	return &CodedError{Code: code, Wrapped: err}
}

func ErrorCode(err error) (string, bool) {
	var coded *CodedError
	if errors.As(err, &coded) {
		return coded.Code, true
	}
	return "", false
}

func errorCodeOr(err error, fallback string) string {
	if code, ok := ErrorCode(err); ok {
		return code
	}
	return fallback
}
