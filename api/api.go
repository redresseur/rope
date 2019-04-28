package api

import "github.com/redresseur/rope/knot"

// 用于同步远程和管理本地记录
type Weaver interface {
	Pull(Rope *knot.Rope)(*knot.Rope, error)
	Push(Rope *knot.Rope)error
	Merge(LocalRope, RemoteRope *knot.Rope) (*knot.Rope, error)
	Add(Rope *knot.Rope, transaction... *knot.Transaction)
}