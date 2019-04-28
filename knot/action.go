package knot

type ActionType int32
type ActionVersion float32

const (
	Create ActionType = 1 << iota
	Update
	Read
	Delete
)

// Action 记录具体修改数据的操作
type Action struct {
	Type ActionType
	Version  ActionVersion
	Data []byte
}

