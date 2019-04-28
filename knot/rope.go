package knot

import (
	"github.com/redresseur/rope/errors"
	"strconv"
	"sync"
)

const Stable_Tx_ID  = "stable_tx_id"
const End_Tx_ID  = "end_tx_id"
const Header_Tx_ID  = "header_tx_id"


type RopeStatus byte

const (
	RopeLocal RopeStatus = 1 << iota
	RopeRemote
)

type Rope struct {
	sync.Mutex
	// 维持交易之间的状态和关系
	Knots map[string]*Transaction
	ID string
	Status RopeStatus
}

func (rp *Rope)Tail()*Transaction{
	return rp.Knots[End_Tx_ID]
}

// 查找起始的交易
func (rp *Rope)Header()*Transaction {
	return rp.Knots[Header_Tx_ID]
}


// 检查交易ID 防止冲突
func (rp *Rope)CheckID(transactionID string) string {

	for i:=0; ;i++{
		if _, isExist := rp.Knots[transactionID]; isExist{
			// 如果交易ID已经存在，就在后面顺延加1
			transactionID = transactionID + "-" + strconv.FormatInt(int64(i), 16)
		}else {
			break
		}
	}

	return transactionID
}

func (rp *Rope)setHeader(transaction *Transaction) {
	transaction.ID = rp.CheckID(transaction.ID)
	rp.Knots[Header_Tx_ID] = transaction
	rp.Knots[transaction.ID] = transaction
}

func (rp *Rope)AppendTransaction(transaction... *Transaction){
	for _, tx := range transaction{
		if rp.Header() != nil{
			tx.ID = rp.CheckID(tx.ID)
			tail := rp.Tail()
			tx.Front = append(tx.Front, tail)
			tail.Next = append(tail.Next, tx)

			// 加入映射表中
			rp.Knots[tx.ID] = tx
			rp.Knots[End_Tx_ID] = tx
		}else {
			rp.setHeader(tx)
		}
	}
}

func (rp *Rope)SetStableTransaction(transaction *Transaction) error {
	if _, isExist := rp.Knots[transaction.ID]; ! isExist{
		return errors.Errorf(errors.ErrorTransactionNotFound, transaction.ID, rp.ID)
	}

	rp.Knots[Stable_Tx_ID] = transaction
	return nil
}

// 获取stableTx, 如果没有就返回Header
func (rp *Rope)StableTransaction() *Transaction {
	if rp.Knots[Stable_Tx_ID] != nil{
		return rp.Knots[Stable_Tx_ID]
	}

	return rp.Header()
}

func (rp *Rope)IsEmpty()bool{
	return len(rp.Knots) == 0
}

func (rp *Rope)IsLocal()bool {
	return rp.Status == RopeLocal
}

func (rp *Rope)GetTransactionByID(transcationID string)*Transaction{
	return rp.Knots[transcationID]
}

func CopyRope(dst *Rope, src *Rope){

}