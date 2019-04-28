package errors

import "fmt"

type RopeErrorType int64

const (
	ErrorNotValid RopeErrorType = 1 << iota
	ErrorTransactionNotFound
	ErrorStableTxNotFoundInRemote
	ErrorSatbleTxIsMerger
	ErrorRopeNotLocal
	ErrorCommon
)

var errorInfo = map[RopeErrorType] string{
	ErrorTransactionNotFound : "the transaction <%s> is not found in the rope <%s>",
	ErrorRopeNotLocal : "the rope <%s> is not local",
	ErrorStableTxNotFoundInRemote: "the stable transaction <%s> of the rope <%s> is not " +
		"found in the remote rope <%s>",
	ErrorSatbleTxIsMerger: "the stable transaction <%s> of the rope <%s> is merger",
	ErrorCommon: "",
}

type RopeError struct {
	errorType RopeErrorType
	error
}

func Errorf(errorType RopeErrorType, args... interface{}) *RopeError {
	format, isExists := errorInfo[errorType]
	if !isExists{
		panic("the errorType: " + string(errorType) + " is not support")
	}else if errorType == ErrorCommon{
		var isOk bool
		if format, isOk = args[0].(string); isOk{
			args = args[1:]
		}else {
			panic("the first args must be string type with Common type")
		}
	}

	return &RopeError{errorType: errorType, error: fmt.Errorf(format, args...)}
}

func ErrorType(err interface{}) RopeErrorType {
	if ropeErr, isOk := err.(*RopeError); isOk{
		return ropeErr.errorType
	}

	return ErrorNotValid
}