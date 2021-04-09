package hashingCycle
type HashingCycle interface {
	Find(key string)*VirtualNode
	FindWithPre(key string)(pre *VirtualNode,cur *VirtualNode)
	FindWithPreAndNext(key string)(pre *VirtualNode,cur *VirtualNode,next *VirtualNode)
	Delete(node string)
    Insert(node string)
	Print()
}