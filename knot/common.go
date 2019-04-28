package knot

import "github.com/satori/go.uuid"

func GenerateRopeID() string {
	return uuid.Must(uuid.NewV1()).String()
}

// Transaction ID 生成规则：
// rope的Id + Action的hash值
func GenerateTransactionID(ropeID string)string{
	return uuid.NewV5(uuid.Must(uuid.FromString(ropeID)),uuid.Must(uuid.NewV1()).String()).String()
}