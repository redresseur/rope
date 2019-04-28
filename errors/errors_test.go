package errors

import "testing"

func TestErrorf(t *testing.T) {
	err := Errorf(ErrorTransactionNotFound, "tx_1", "rope_1")
	t.Logf(err.Error())

	err = Errorf(ErrorCommon, "Hello %s, are you ok !?", "Lilei")
	t.Logf(err.Error())
}

func TestErrorType(t *testing.T) {
	err := Errorf(ErrorTransactionNotFound, "tx_1", "rope_1")
	if ErrorType(err) != ErrorTransactionNotFound{
		t.Fatalf("the error's type is not correct")
	}
}