package hashingCycle

import (
	uuid "github.com/satori/go.uuid"
	"testing"
	)
func TestSliceHashingCycle(t *testing.T){
	server1:="192.168.0.1:8080"
	server2:="192.168.0.2:8080"
	server3:="192.168.0.3:8080"

	nodes := []string{server1,server2,server3}
	cycle:= NewSliceHashingCycle(nodes,4096)
	jobSize:=10000

	nodeMap:=make(map[string]int)
	jobs:=make(map[string]string,jobSize)

	//t.Log(cycle.vns)

	for i:=0;i<jobSize;i++{
		u,_ := uuid.NewV4()
		jobId := u.String()

		n := cycle.Find(jobId)
		node:=n.node
		jobs[jobId]=node
		nodeMap[node]=nodeMap[node]+1
	}
	for k,v:=range nodeMap{
		println(k+":",v)
	}

	//t.Log(cycle.vns)
	cycle.Delete(server3)
	println("server3 deleted...")
	//t.Log(cycle.vns)
	nodeMap=make(map[string]int)

	unchanged:=make(map[string]int,10)
	jobs1:=make(map[string]string,jobSize)
	for jobId:=range jobs{
		n := cycle.Find(jobId)
		node:=n.node
		if jobs[jobId] == node {
			unchanged[node]++
		}
		jobs1[jobId]=node
		nodeMap[node]=nodeMap[node]+1
	}
	for k,v:=range nodeMap{
		println(k+":",v)
	}
	println("unchanged ...")
	for k,v:=range unchanged{
		println(k+":",v)
	}

	server4:="192.168.0.4:8080"
	//t.Log(cycle.vns)
	cycle.Insert(server4)
	//t.Log(cycle.vns)
	println("\nserver4 inserted...")
	unchanged=make(map[string]int,10)

	nodeMap=make(map[string]int)

	for jobId:=range jobs{
		n := cycle.Find(jobId)
		node:=n.node
		if jobs1[jobId] == node {
			unchanged[node]++
		}
		//println("jobId:"+jobId,"hash:",hash(jobId))
		nodeMap[node]=nodeMap[node]+1
	}
	for k,v:=range nodeMap{
		println(k+":",v)
	}
	println("unchanged ...")
	for k,v:=range unchanged{
		println(k+":",v)
	}


}