package etcd

import (
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
	"os"
	"testing"
	"time"
)

func getEtcd()(etcd *Etcd){
	var err error
	if etcd, err = NewEtcd(&[]string{"127.0.0.1:2379"});err!=nil{
		println(err.Error())
		os.Exit(1)
	}
	return
}
func TestJobCreate(t *testing.T) {
	etcd := getEtcd()
	etcd.Put("jobs.cmd/JobCreate/10", "")
	etcd.Put("jobs.cmd/JobCreate/20", "")
	etcd.Put("jobs.cmd/JobCreate/30", "")
}
func TestJobDelete(t *testing.T) {
	etcd := getEtcd()
	etcd.Put("jobs.cmd/JobDelete/10", "")
	//etcd.Put("jobs.cmd/JobCreate/20", "")
	//etcd.Put("jobs.cmd/JobCreate/30", "")
}
func TestJobUpdate(t *testing.T) {
	etcd := getEtcd()
	etcd.Put("jobs.cmd/JobUpdate/10", "abc")
	//etcd.Put("jobs.cmd/JobCreate/20", "")
	//etcd.Put("jobs.cmd/JobCreate/30", "")
}
func TestJobSuspend(t *testing.T) {
	etcd := getEtcd()
	etcd.Put("jobs.cmd/JobSuspend/10", "")
	//etcd.Put("jobs.cmd/JobCreate/20", "")
	//etcd.Put("jobs.cmd/JobCreate/30", "")
}
func TestJobResume(t *testing.T) {
	etcd := getEtcd()
	etcd.Put("jobs.cmd/JobResume/10", "")
	//etcd.Put("jobs.cmd/JobCreate/20", "")
	//etcd.Put("jobs.cmd/JobCreate/30", "")
}
func TestSet(t *testing.T) {
	etcd := getEtcd()
	etcd.Put("nodes/client1", "")
	etcd.Put("nodes/client2", "")
}
func TestGet1(t *testing.T) {
	etcd := getEtcd()
	_, _ = etcd.Put("jobs.version/1", "123")
	//println(string(resp0.PrevKv.Key),":",string(resp0.PrevKv.Value))

	resp, e := etcd.Get("jobs.version/1")
	if e!=nil{
		println(e.Error())
	}
	kv := resp.Kvs[0]
	println("key",string(kv.Key),"value",string(kv.Value))
}
func TestSetWithLease(t *testing.T) {
	etcd := getEtcd()
	var err error
	var resp *clientv3.LeaseGrantResponse
	if resp, err = etcd.LeaseGrant(5);err!=nil{
		println(err.Error())
		return
	}
	etcd.Put("nodes/client1", "",clientv3.WithLease(resp.ID))
}

func TestSetWithKeepAlive(t *testing.T) {
	etcd := getEtcd()
	var err error
	var resp *clientv3.LeaseGrantResponse
	if resp, err = etcd.LeaseGrant(5);err!=nil{
		println(err.Error())
		return
	}
	var ch <-chan *clientv3.LeaseKeepAliveResponse
	if ch, err = etcd.KeepAlive(resp.ID);err!=nil{
		println(err.Error())
		return
	}
	if _, err = etcd.Put("nodes/client1", "", clientv3.WithLease(resp.ID));err!=nil{
		println(err.Error())
		return
	}
	var resp1 *clientv3.LeaseKeepAliveResponse
	for{
		select {
		case resp1 =<-ch:
			if resp1==nil{
				println("失效")
				return
			}else{
				println("收到租约应答",resp1.ID)
			}
		}
	}

}
func TestDelete(t *testing.T) {
	etcd := getEtcd()
	etcd.Delete("nodes/client1",clientv3.WithPrefix())
}
func TestGet(t *testing.T){
	etcd := getEtcd()
	var resp *clientv3.GetResponse
	var err error
	for range time.Tick(1*time.Second){
		if resp, err = etcd.Get("nodes", clientv3.WithPrefix());err!=nil{
			println(err.Error())
		}
		println("len(kvs)",len(resp.Kvs))
		for i,val:= range resp.Kvs{
			println("key",i)
			println("suffix",string(val.Key[6:]))
			println("key",string(val.Key),"val",string(val.Value),"lease",val.Lease,"create",val.CreateRevision,"mod",val.ModRevision,"version",val.Version)
			println("res.count",resp.Count,"header.revision",resp.Header.Revision)
		}
	}
}

func TestWatch(t *testing.T) {
	etcd := getEtcd()
	etcd.Watch("nodes", func(event *clientv3.Event) {
		println("type",event.Type.String(),"key",string(event.Kv.Key))
	},clientv3.WithPrefix())
	make(chan int)<-1
}

func  TestLease(t *testing.T){
	etcd := getEtcd()
	resp, _ := etcd.newLease().Grant(context.TODO(), 5)
	go func() {
		for range time.Tick(4*time.Second){
			etcd.Put("lease.test","haha",clientv3.WithLease(resp.ID))
		}
	}()
	for range time.Tick(1*time.Second){

		resp, _ := etcd.Get("lease.test")

		kvs := resp.Kvs
		println(len(kvs))
		//kv := kvs[0]
		//println(string(kv.Key)," ",string(kv.Value))
	}
}

func  TestKeepAliveOnce(t *testing.T){
	etcd := getEtcd()
	resp, _ := etcd.newLease().Grant(context.TODO(), 5)
	etcd.Put("lease.test","haha",clientv3.WithLease(resp.ID))

	go func() {
		for range time.Tick(4*time.Second){
			etcd.KeepAliveOnce(resp.ID)
		}
	}()
	println("now:",time.Now().Second() )
	for range time.Tick(1*time.Second){
		resp, _ := etcd.Get("lease.test")

		kvs := resp.Kvs
		println(len(kvs))
		println("now:",time.Now().Second() )

		//kv := kvs[0]
		//println(string(kv.Key)," ",string(kv.Value))
	}
}