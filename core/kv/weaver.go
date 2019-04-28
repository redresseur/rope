package kv

import (
	"github.com/op/go-logging"
	"github.com/redresseur/rope/errors"
	"github.com/redresseur/rope/knot"
)

var logger = logging.MustGetLogger("core/kv")

type kvActionFilter struct {
	createFilter map[string]bool
	deleteFilter map[string]bool
	updateFilter map[string]bool
	readFilter map[string]bool // 暂时没什么用途
}

type PullFunc func(*knot.Rope)(error)

// 用于key-value 数据模型
type WeaverKV struct {
	pull PullFunc
	handler Handler
}

type WeaverKVOption  func(*WeaverKV)

func WithPullFunc(pull PullFunc) WeaverKVOption{
	return func(kv *WeaverKV) {
		kv.pull = pull
	}
}

func WithDataSetsHandler(handler Handler) WeaverKVOption {
	return func(kv *WeaverKV) {
		kv.handler = handler
	}
}
// TODO : 添加一个实力化接口函数
//func NewWeaverKV(options... WeaverKVOption) api.Weaver {
//
//}

func (kv *WeaverKV)Add(Rope *knot.Rope, transaction... *knot.Transaction) error {
	Rope.Lock()
	defer Rope.Unlock()
	if ! Rope.IsLocal(){
		return errors.Errorf(errors.ErrorRopeNotLocal, Rope.ID)
	}

	Rope.AppendTransaction(transaction...)
	return nil
}



func (kv *WeaverKV)Merge(LocalRope, RemoteRope *knot.Rope)(*knot.Rope, error) {
	// 加锁
	LocalRope.Lock()
	defer LocalRope.Unlock()

	// TODO: 检查ID是否一致

	// 如果为空则全部复制
	if LocalRope.IsEmpty(){
		knot.CopyRope(LocalRope, RemoteRope)
		//TODO: 与本地合并
		//TODO: 从远程拉取
		return LocalRope, nil
	}

	// 取stable transaction
	stableTx := LocalRope.StableTransaction()
	// stableTx 不可以是冲突交易
	if stableTx.IsMerge{
		return nil, errors.Errorf(errors.ErrorSatbleTxIsMerger,
			stableTx.ID, LocalRope.ID)
	}

	remoteStableTx := RemoteRope.GetTransactionByID(stableTx.ID)
	if  remoteStableTx == nil{
		return nil, errors.Errorf(errors.ErrorStableTxNotFoundInRemote,
			remoteStableTx.ID, LocalRope.ID, RemoteRope.ID)
	}

	// 从本地的stableTx 向后遍历，确认本地的更新
	tx  := stableTx
	localFilter := kvActionFilter{}
	for ;len(tx.Next) != 0; {
		for _, nextTx := range tx.Next {
			if nextTx.IsMerge{
				// 冲突的Tx跳过不处理
				tx = nextTx
				continue
			}else {
				kv.actionFilter(tx, &localFilter)
			}
		}
	}

	// 从远程的rope中的stableTx 向后遍历来确认远程的更新
	// 注意是本地rope stableTx 对应的 远程 tx， 不是远程 rope 的 stableTx
	if remoteStableTx.IsMerge {
		return nil, errors.Errorf(errors.ErrorSatbleTxIsMerger,
			stableTx.ID, RemoteRope.ID)
	}

	tx = remoteStableTx
	remoteFilter := kvActionFilter{}
	for ;len(tx.Next) != 0; {
		for _, nextTx := range tx.Next {
			if nextTx.IsMerge{
				// 冲突的Tx跳过不处理
				tx = nextTx
				continue
			}else {
				kv.actionFilter(tx, &remoteFilter)
			}
		}
	}

	// 合并 远程和本地的 rope 记录

	// 统计需要从远端rope中拉取的值


	return nil, nil
}

func (kv *WeaverKV)generatePullList(localFilter, remoteFilter *kvActionFilter,
	pullList map[string]knot.ActionType) error  {
	// 处理 remote.create
	for remoteKey, isCreated := range remoteFilter.createFilter {
		if ! isCreated {
			continue
		}

		if isCreated, isOK := localFilter.createFilter[remoteKey]; isOK && isCreated{
			// 本地和远程 同时创建时，备份本地的key
			backUpKey, err := kv.handler.BackUp(remoteKey)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.createFilter[backUpKey] = true
			localFilter.createFilter[remoteKey] = false
			pullList[remoteKey] = knot.Create
			continue
		}

		if isUpdate, isOK := localFilter.updateFilter[remoteKey]; isOK && isUpdate{
			// 处理同上
			backUpKey, err := kv.handler.BackUp(remoteKey)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.updateFilter[backUpKey] = true
			localFilter.updateFilter[remoteKey] = false
			pullList[remoteKey] = knot.Create
			continue
		}

		if isDelete, isOK := localFilter.deleteFilter[remoteKey]; isOK && isDelete{
			// 此时不从远程拉取
			continue
		}
	}

	// 处理updateFilter
	for remoteKey, isUpdate := range remoteFilter.updateFilter{
		if ! isUpdate{
			continue
		}

		if isCreated, isOK := localFilter.createFilter[remoteKey]; isOK && isCreated{
			// 本地和远程 同时创建时，备份本地的key
			backUpKey, err := kv.handler.BackUp(remoteKey)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.createFilter[backUpKey] = true
			localFilter.createFilter[remoteKey] = false
			pullList[remoteKey] = knot.Update
			continue
		}

		if isUpdate, isOK := localFilter.updateFilter[remoteKey]; isOK && isUpdate{
			// 处理同上
			backUpKey, err := kv.handler.BackUp(remoteKey)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.updateFilter[backUpKey] = true
			localFilter.updateFilter[remoteKey] = false
			pullList[remoteKey] = knot.Update
			continue
		}

		if isDelete, isOK := localFilter.deleteFilter[remoteKey]; isOK && isDelete{
			// 此时不从远程拉取
			continue
		}
	}

	// 处理deleteFilter
	for remoteKey, isDelete := range remoteFilter.deleteFilter{
		if ! isDelete {
			continue
		}

		if isCreated, isOK := localFilter.createFilter[remoteKey]; isOK && isCreated{
			// 远程删除 本地未删除，加入回收站
			err := kv.handler.Delete(remoteKey, true)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.createFilter[remoteKey] = false
			continue
		}

		if isUpdate, isOK := localFilter.updateFilter[remoteKey]; isOK && isUpdate{
			// 远程删除 本地未删除，加入回收站
			err := kv.handler.Delete(remoteKey, true)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.updateFilter[remoteKey] = false
			continue
		}

		if isDelete, isOK := localFilter.createFilter[remoteKey]; isOK && isDelete{
			// 远程删除 本地也删除，则状态一致，后面无需判断
			// 更新过滤器状态
			localFilter.deleteFilter[remoteKey] = false
			continue
		}
	}

	return nil
}


func (kv *WeaverKV)generatePushList(localFilter, remoteFilter *kvActionFilter,
	pushList map[string]knot.ActionType) error  {
	// 处理 local.create
	for localKey, isCreated := range localFilter.createFilter {
		if ! isCreated {
			continue
		}

		if isCreated, isOK := remoteFilter.createFilter[localKey]; isOK && isCreated{
			// 本地和远程 同时创建时，备份本地的key
			backUpKey, err := kv.handler.BackUp(localKey)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.createFilter[backUpKey] = true
			localFilter.createFilter[localKey] = false
			pushList[backUpKey] = knot.Create
			continue
		}

		if isUpdate, isOK := remoteFilter.updateFilter[localKey]; isOK && isUpdate{
			// 处理同上
			backUpKey, err := kv.handler.BackUp(localKey)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.createFilter[backUpKey] = true
			localFilter.createFilter[localKey] = false
			pushList[backUpKey] = knot.Create
			continue
		}

		if isDelete, isOK := remoteFilter.deleteFilter[localKey]; isOK && isDelete{
			// 远程已经删除， 本地尚未删除
			// 加入回收站
			if err := kv.handler.Delete(localKey, true); err != nil{
				return err
			}
			localFilter.createFilter[localKey] = false
			pushList[localKey] = knot.Delete
			continue
		}
	}

	// 处理updateFilter
	for localKey, isUpdate := range localFilter.updateFilter{
		if ! isUpdate {
			continue
		}

		if isCreated, isOK := remoteFilter.createFilter[localKey]; isOK && isCreated{
			// 本地和远程 同时创建时，备份本地的key
			backUpKey, err := kv.handler.BackUp(localKey)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.updateFilter[backUpKey] = true
			localFilter.updateFilter[localKey] = false
			pushList[backUpKey] = knot.Update
			continue
		}

		if isUpdate, isOK := remoteFilter.updateFilter[localKey]; isOK && isUpdate{
			// 处理同上
			backUpKey, err := kv.handler.BackUp(localKey)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			localFilter.updateFilter[backUpKey] = true
			localFilter.updateFilter[localKey] = false
			pushList[backUpKey] = knot.Update
			continue
		}

		if isDelete, isOK := remoteFilter.deleteFilter[localKey]; isOK && isDelete{
			// 远程已经删除， 本地尚未删除
			// 加入回收站
			if err := kv.handler.Delete(localKey, true); err != nil{
				return err
			}
			localFilter.updateFilter[localKey] = false
			pushList[localKey] = knot.Delete
			continue
		}
	}

	// 处理deleteFilter
	for localKey, isDelete := range localFilter.deleteFilter{
		if ! isDelete {
			continue
		}

		if isCreated, isOK := remoteFilter.createFilter[localKey]; isOK && isCreated{
			// 远程未删除 本地已经删除，远程的也要删除
			pushList[localKey] = knot.Delete
			continue
		}

		if isUpdate, isOK := remoteFilter.updateFilter[localKey]; isOK && isUpdate{
			// 远程删除 本地未删除，加入回收站
			err := kv.handler.Delete(localKey, true)
			if err != nil{
				return err
			}

			// 更新过滤器状态
			pushList[localKey] = knot.Delete
			continue
		}

		if isDelete, isOK := localFilter.createFilter[localKey]; isOK && isDelete{
			// 远程删除 本地也删除，则状态一致，后面无需判断
			// 更新过滤器状态
			//localFilter.deleteFilter[remoteKey] = false
			continue
		}
	}

	return nil
}

func (kv *WeaverKV)mergeFilter(localFilter, remoteFilter *kvActionFilter) error  {
	// 等待拉取的key 列表
	pullList := map[string]knot.ActionType{}
	// 等待推送的 key 列表
	pushList := map[string]knot.ActionType{}

	// Setup1 先统计需要拉取的
	if err := kv.generatePullList(localFilter, remoteFilter, pullList); err != nil{
		return err
	}

	if err := kv.generatePushList(localFilter, remoteFilter, pushList);err != nil{
		return err
	}

	return nil
}

// actionFilter 这是一个重要的函数，它主要的功能
// 是为了统计传入的tx，最后统计出变动的key
func (kv *WeaverKV)actionFilter(transaction *knot.Transaction, filter *kvActionFilter){
	for _, action := range transaction.Actions {
		// 在 kvWeaver 里面 Action Data存储的即为 key
		keyID := string(action.Data)
		switch action.Type{
		case knot.Create:
			// 先判断 deleteFilter 中是否存在
			if _, isOk := filter.deleteFilter[keyID]; isOk{
				// 如果deleteFilter中存在则置为false
				filter.deleteFilter[keyID] = false
				// filter.createFilter[keyID] = true
			}
			filter.createFilter[keyID] = true
		case knot.Delete:
			// 先判断createFilter中是否存在
			if isCreated, isOk := filter.createFilter[keyID]; isOk{
				// 如果存在则标记为 false
				// 同时deleteFilter 中不添加
				filter.createFilter[keyID] = false
				// key的完整生命周期在冲突的记录范围内，则视为该key不存在
				if !isCreated{
					// 判断updateFilter 中是否存在
					if _, isOk := filter.updateFilter[keyID]; isOk {
						filter.updateFilter[keyID] = false
					}
				}
			}else {
				filter.deleteFilter[keyID] = true
			}
		case knot.Update:
			// 先判断createFilter中是否存在
			if _, isOk := filter.createFilter[keyID]; isOk{
				// 如果存在将其置为false
				filter.createFilter[keyID] = false
			}

			filter.updateFilter[keyID] = true
		case knot.Read:
			filter.readFilter[keyID] = true
		default:
			logger.Debugf("the type %d is not support", action.Type)
		}
	}
}
