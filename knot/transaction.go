package knot

// Transaction 纪录修改数据集的单元
type Transaction struct {
	ID string
	Front []*Transaction
	Next []*Transaction
	Actions []*Action

	// 是在分歧中的节点
	//	1-2-3-4-5-6
	//   \2.1-3.1/
	// 其中2.1、3.1以及2、3、4、5都是分歧节点
	IsMerge bool
}

func (tx *Transaction)AddAction(actions... *Action){
	tx.Actions = append(tx.Actions, actions...)
}

func (tx *Transaction)SetMergeFlag(isMerger bool)  {
	tx.IsMerge = isMerger
}
