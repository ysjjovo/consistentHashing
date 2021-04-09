package hashingCycle

import (
	"fmt"
	"sort"
)

type VirtualNode struct {
	hash  int
	node  string
	index int
}

type SliceHashingCycle struct {
	replicas int
	vns      []*VirtualNode
	//mutex sync.RWMutex
}

//新建哈希环
func NewSliceHashingCycle(nodes []string, replicas int) HashingCycle {
	cycle := &SliceHashingCycle{replicas: replicas, vns: make([]*VirtualNode, 0, len(nodes)*replicas)}
	for _,node:=range nodes{
		for j:=0;j<replicas;j++{
			cycle.vns=append(cycle.vns,&VirtualNode{node: node, index: j, hash: cycle.calHash(node,j)})
		}
	}
	sort.Sort(cycle)
	return cycle
}
func (h *SliceHashingCycle) Find(key string)*VirtualNode{
	idx:=h.getIdx(key)
	if idx==-1{
		return nil
	}
	return h.vns[idx]
}
func (h *SliceHashingCycle) FindWithPre(key string)(pre *VirtualNode,cur *VirtualNode){
	idx:=h.getIdx(key)
	if idx==-1{
		return nil,nil
	}
	preIdx:=idx-1
	size := len(h.vns)
	if preIdx == -1 {
		preIdx= size -1
	}
	return h.vns[preIdx],h.vns[idx]
}
func (h *SliceHashingCycle) FindWithPreAndNext(key string)(pre *VirtualNode,cur *VirtualNode,next *VirtualNode){
	idx:=h.getIdx(key)
	if idx==-1{
		return nil,nil,nil
	}
	preIdx:=idx-1
	size := len(h.vns)
	if preIdx == -1 {
		preIdx= size -1
	}

	nextIdx := idx + 1
	for{
		if nextIdx == size {
			nextIdx = 0
		}
		if h.vns[nextIdx].node != h.vns[idx].node {
			break
		}
		nextIdx++
	}

	return h.vns[preIdx],h.vns[idx],h.vns[nextIdx]
}

func (h *SliceHashingCycle) Print() {
	println("virtual nodes---")
	for _,v := range h.vns {
		println("node:",v.node,"index",v.index,"hash:",v.hash)
	}
	println("virtual nodes---")
}
func (h *SliceHashingCycle) getIdx(key string) int{
	//defer h.mutex.RUnlock()
	//h.mutex.RLock()
	size := len(h.vns)
	if size == 0 {
		return -1
	}
	idx := h.search(Hash(key), 0, size-1)
	//println("get idx",idx)
	if idx == size {
		idx = 0
	}
	return idx
}

//插入结点
func (h *SliceHashingCycle) Insert(node string) {
	//defer h.mutex.Unlock()
	//h.mutex.Lock()
	for i := 0; i < h.replicas; i++ {
		hash:=h.calHash(node, i)
		size := len(h.vns)
		vn := &VirtualNode{node: node, index: i, hash: hash}
		//println("new vn:",vn.node,vn.hash)
		if size == 0 {
			h.vns=append(h.vns,vn)
			continue
		}
		idx := h.search(hash,0, size-1)
		//println("find insert place",idx,hash)
		if idx == size {//追加
			h.vns=append(h.vns,vn)
			continue
		} else if h.vns[idx].hash == hash {//不覆盖
			continue
		}
		//插入
		tmp:=append([]*VirtualNode{},h.vns[idx:]...)
		h.vns = append(append(h.vns[:idx], vn),tmp...)
	}
}

//删除结点
func (h *SliceHashingCycle) Delete(node string) {
	//defer h.mutex.Unlock()
	//h.mutex.Lock()
	size := len(h.vns)
	//println("end:",end)
	var i int
	for ; i <size; i++ {
		if h.vns[i].node == node {
			break
		}
	}
	//println("first to delete:",i)
	for j := i + 1; j < size; j++ {
		if h.vns[j].node == node {
			continue
		}
		h.vns[i] = h.vns[j]
		i++
	}
	h.vns = h.vns[0 : i]
}

//i<=j
func (h *SliceHashingCycle) search(hash, i, j int) int {
	//println(i,j)
	mid := (i + j) / 2
	midValue := h.vns[mid].hash
	if hash == midValue {
		return mid
	} else if hash < midValue {
		if mid == i  {
			return mid
		}
		return h.search(hash, i, mid-1)
	} else {
		if mid == j {
			mid ++
			return mid
		}
		return h.search(hash, mid+1, j)
	}
}
func (h *SliceHashingCycle) calHash(node string, i int) int {
	return Hash(fmt.Sprintf("%s%d", node, i))
}
func (h *VirtualNode) GetHash() int {
	return h.hash
}

func (h *VirtualNode) GetNode() string {
	return h.node
}

//排序
func (h *SliceHashingCycle) Sort() {
	sort.Sort(h)
}
func (h *SliceHashingCycle) Len() int { return len(h.vns) }

func (h *SliceHashingCycle) Swap(i, j int) { h.vns[i], h.vns[j] = h.vns[j], h.vns[i] }

func (h *SliceHashingCycle) Less(i, j int) bool { return h.vns[i].hash < h.vns[j].hash }
