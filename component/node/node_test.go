package node

import (
	"consistentHashing/component"
	"consistentHashing/component/etcd"
	"consistentHashing/component/hashingCycle"
	"consistentHashing/component/job"
	"os"
	"testing"
	"time"
)

const (
	NodePrefix="nodes"
	JobPrefix="jobs.cmd"
)

func getEtcd()(etc *etcd.Etcd){
	var err error
	if etc, err = etcd.NewEtcd(&[]string{"127.0.0.1:2379"});err!=nil{
		println(err.Error())
		os.Exit(1)
	}
	return
}
//func getNode()*Node{
//	return NewNode(getEtcd())
//}
//2-3-1

func TestJobMapToNode(t *testing.T){
	cycle := hashingCycle.NewSliceHashingCycle([]string{"2","3"}, 128)
	cur := cycle.Find("2")
	println(cur.GetNode())
}
func TestNode1(t *testing.T){
	component.InitComponents()
	node := NewNode(getEtcd(),&Config{nodePrefix: NodePrefix,jobPrefix:JobPrefix,name:"1",ttl:5,replicas:128,eventCapacity:10})
	println("initing...")
	node.initNode()
	println("inited...")
	checkStatusSimple(node)
	//checkStatus(node)
}

func TestNode2(t *testing.T){
	component.InitComponents()
	node := NewNode(getEtcd(),&Config{nodePrefix: NodePrefix,jobPrefix:JobPrefix,name:"2",ttl:5,replicas:128,eventCapacity:10})
	println("initing...")
	node.initNode()
	println("inited...")
	checkStatusSimple(node)
	//checkStatus(node)
}

func TestNode3(t *testing.T){
	component.InitComponents()
	node := NewNode(getEtcd(),&Config{nodePrefix: NodePrefix,jobPrefix:JobPrefix,name:"3",ttl:5,replicas:128,eventCapacity:10})
	println("initing...")
	node.initNode()
	println("inited...")
	checkStatusSimple(node)
	//checkStatus(node)
}
func checkStatus(node *Node){
	for range time.Tick(2*time.Second){
		//nodes, err := node.getLiveNodes()
		//if err!=nil{
		//	println(err.Error())
		//	os.Exit(1)
		//}
		//print("nodes:")
		//for _,v:=range nodes{
		//	print(v," ")
		//}
		//node.cycle.Print()
		print("jobs: ")

		for _,v := range node.jobs.FindJobs(0,int(^uint(0) >> 1)){
			print(v.Id," ")
		}
		println()
		println("myJobs:")

		cnt:=0
		for _,v := range job.Wheel.Jobs() {
			print(v.Id," hash:",v.Hash)
			resp, err := component.Etcd.Get("jobs.version/" + v.Id)
			if err != nil {
				print(err.Error())
				return
			}
			ver := string(resp.Kvs[0].Value)
			if ver == v.Version {
				cnt+=1
			} else {
				print(" valid,ver:",v.Version,",remote ver:",ver)
			}
			println()
		}
		println("cnt:",cnt)
	}
}

func checkStatusSimple(node *Node){
	for range time.Tick(2*time.Second) {
		cnt := 0
		job.Wheel.Mu.RLock()
		for _, v := range job.Wheel.Jobs() {
			resp, err := component.Etcd.Get("jobs.version/" + v.Id)
			if err != nil {
				print(err.Error())
				return
			}
			cur := node.cycle.Find(v.Id)

			if cur.GetNode() == node.config.name {
				ver := string(resp.Kvs[0].Value)
				if ver == v.Version {
					cnt++
				}else{
					println(" wrong schedule,id:", v.Id, "hash:", v.Hash)
				}
			}
		}
		job.Wheel.Mu.RUnlock()
		println("scheduling jobs cnt:", cnt)
	}
}
