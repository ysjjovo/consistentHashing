package node

import (
	"consistentHashing/component"
	"consistentHashing/component/etcd"
	"consistentHashing/component/hashingCycle"
	"consistentHashing/component/job"
	"fmt"
	client "go.etcd.io/etcd/clientv3"
	"os"
	"strings"
)
type Node struct {
	config        *Config
	etcd          *etcd.Etcd
	cycle         hashingCycle.HashingCycle
	jobs          *job.SortedJobs
	eventCh       chan *ActionEvent
	eventHandlers map[string]func(*ActionEvent)
}
type ActionEvent struct{
	Name string
	Id   string
	Val  string
}


type Config struct{
	nodePrefix    string
	jobPrefix     string
	name          string
	ttl           int64
	replicas      int
	eventCapacity int
}

func NewNode(etcd *etcd.Etcd,config *Config)(node *Node){
	sortedJob := job.NewSortedJob()
	node=&Node{
		etcd:    etcd,
		config:  config,
		jobs:    sortedJob,
		eventCh: make(chan *ActionEvent,config.eventCapacity),
		eventHandlers:make(map[string]func(*ActionEvent),16),
	}
	return
}
func (n *Node)initNode(){
	n.keepAlive()
	n.initHashCycle()
	n.loadMyJobs()
	go n.watchJob()
	n.registerHandlers()
	go n.handleActionEvent()
}
func (n *Node)getLiveNodes()(nodes []string,err error){
	var resp *client.GetResponse
	if resp, err = n.etcd.Get(n.config.nodePrefix,client.WithPrefix());err!=nil{
		return
	}
	nodes=[]string{}
	for _,v:=range resp.Kvs{
		//println("originKey",string(v.Key))
		nodes=append(nodes,n.getNodeName(v.Key))
	}
	return
}
func (n *Node)getNodeName(key []byte)string{
	return string(key[len(n.config.nodePrefix)+1:])
}
func (n *Node)watchLiveNodes(){
	n.etcd.Watch(n.config.nodePrefix, func(event *client.Event) {
		name := n.getNodeName(event.Kv.Key)
		if event.Type==client.EventTypePut{
			n.eventCh <- &ActionEvent{Name: NodeOnline, Id:name}
		} else {
			n.eventCh <- &ActionEvent{Name: NodeOffline, Id:name}
		}
	},client.WithPrefix())
}
func (n *Node)watchJob(){
	component.Etcd.Watch(n.config.jobPrefix, func(event *client.Event) {
		kv := event.Kv
		k:=string(kv.Key[len(n.config.jobPrefix)+1:])
		ks:=strings.Split(k,"/")
		if len(ks) != 2 {//不处理
			println("unrecongnized event:",k)
			return
		}
		action,jobId:=ks[0],ks[1]
		//println("action:",action,"jobId:",jobId)
		n.eventCh <- &ActionEvent{Name: action, Id:jobId,Val:string(kv.Value)}
	},client.WithPrefix())
}
func (n *Node) handleActionEvent(){
	for event := range n.eventCh {
		handler:=n.eventHandlers[event.Name]
		if handler == nil{
			println("unrecognized event:",event.Name)
			continue
		}
		println("handling event:",event.Name,"Id:",event.Id,"Val:",event.Val)
		handler(event)
	}
}
func (n *Node)trySchedule(jb *job.Job) {
	cur := n.cycle.Find(jb.Id)
	if cur.GetNode() == n.config.name {
		//需要加入节点
		if err := jb.Schedule();err!=nil{
			println(err)//调度失败
		}
	}
}
func (n *Node)initHashCycle(){
	var nodes []string
	var err error
	if nodes, err = n.getLiveNodes();err!=nil{
		println(err.Error())
		os.Exit(1)
	}
	n.cycle = hashingCycle.NewSliceHashingCycle(nodes, n.config.replicas)
	go n.watchLiveNodes()
}

func (n *Node) keepAlive(){
	resp, err := n.etcd.LeaseGrant(n.config.ttl);
	if err!=nil{
		println(err.Error())
		return
	}
	ch, err := n.etcd.KeepAlive(resp.ID);
	if err!=nil{
		println(err.Error())
		return
	}
	if _, err = n.etcd.Put(n.config.nodePrefix+"/"+n.config.name, "", client.WithLease(resp.ID));err!=nil{
		println(err.Error())
		return
	}
	go func(){
		for{
			select {
			case resp1:=<-ch:
				if resp1==nil{
					println("keepAlive failed")
					//os.Exit(1)
				}
			}
		}
	}()
}
//分别对每个分片加载
func (n *Node) loadMyJobs(){
	//cnt:=0
	for i:=0;i<n.config.replicas;i++{
		vName := fmt.Sprintf("%s%d", n.config.name, i)
		pre,cur:=n.cycle.FindWithPre(vName)
		//println("pre:",pre.GetNode(),"cur:",cur.GetNode())
		jobs := n.jobs.FindJobs(pre.GetHash(), cur.GetHash())
		//cnt+=len(jobs)
		if jobs == nil {
			continue
		}
		for _, j := range jobs{
			if err:= j.Schedule();err!=nil{
				println(err.Error())
				continue
			}
		}
	}
	//println("total loaded:",cnt)
}

//分别对每个分片加载
func (n *Node) takeOver(other string){
	for i:=0;i<n.config.replicas;i++{
		vName := fmt.Sprintf("%s%d", other, i)
		pre,cur,next:=n.cycle.FindWithPreAndNext(vName)
		//println("pre:",pre.GetNode(),"offline node:",cur.GetNode(),"next:",next.GetNode())

		if next.GetNode() == n.config.name {
			jobs := n.jobs.FindJobs(pre.GetHash(), cur.GetHash())
			if jobs == nil {
				continue
			}
			for _, j := range jobs{
				if err:= j.Schedule();err!=nil{
					println(err.Error())
					continue
				}
			}
		}
	}
}
const (
	JobCreate ="JobCreate"
	JobDelete ="JobDelete"
	JobUpdate ="JobUpdate"
	JobSuspend ="JobSuspend"
	JobResume ="JobResume"
	NodeOnline ="NodeOnline"
	NodeOffline ="NodeOffline"
)
func (n *Node) registerHandlers() {
	n.register(JobCreate,func(event *ActionEvent){
		jobId:=event.Id
		n.trySchedule(n.jobs.AddJob(jobId))
	})
	n.register(JobDelete,func(event *ActionEvent){
		jobId:=event.Id
		j := n.jobs.DelJob(jobId)
		if j !=nil {
			if node := n.cycle.Find(j.Id); node != nil && node.GetNode() == n.config.name {
				j.LockVersion()
			}
		}
	})
	n.register(JobUpdate,func(event *ActionEvent){
		jobId:=event.Id
		_, j := n.jobs.Find(jobId)
		if j ==nil {
			return
		}
		newCron := event.Val
		if newCron != j.Cron { //发生变化
			j.Cron= newCron
			n.trySchedule(j)
		}
	})
	n.register(JobSuspend,func(event *ActionEvent){
		jobId:=event.Id
		if _, j := n.jobs.Find(jobId);j !=nil {

			if node := n.cycle.Find(j.Id); node != nil && node.GetNode() == n.config.name {
				j.LockVersion()
			}
		}
	})
	n.register(JobResume,func(event *ActionEvent){
		jobId:=event.Id
		if _, j := n.jobs.Find(jobId);j !=nil {
			n.trySchedule(j)
		}
	})

	n.register(NodeOnline,func(event *ActionEvent){
		node:=event.Id
		n.cycle.Insert(node)
	})
	n.register(NodeOffline,func(event *ActionEvent){
		node:=event.Id
		n.takeOver(node)
		n.cycle.Delete(node)
	})
}
func (n *Node) register(cmd string,handler func(*ActionEvent)) {
	n.eventHandlers[cmd]=handler
}


