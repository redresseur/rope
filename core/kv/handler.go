package kv

// 操纵kv数据集的接口
type Handler interface {
	// 获取数据
	Get(key string) ([]byte, error)

	// 设置数据
	Set(key string, value []byte) error

	// 备份数据， 备份后会返回一个新的 key
	BackUp(key string) (string, error)

	// 回收一个key
	ReCycle(key string) error

	// 删除一个key，如果是可回收的则不彻底删除，不可回收的就彻底删除
	Delete(key string, cycle bool) error
}

