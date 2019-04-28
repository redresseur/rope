package knot

import "testing"

func TestGenerateRopeID(t *testing.T) {
	t.Logf("ROPE ID : %s", GenerateRopeID())
}

func TestGenerateTransactionID(t *testing.T) {
	ropeID := GenerateRopeID()
	t.Logf("ROPE ID %s", ropeID)
	txID := GenerateTransactionID(ropeID)
	t.Logf("Transaction ID %s", txID)
}