package etcd

import (
	client "go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
	"time"
)

type Etcd struct{
	cli *client.Client
}
func NewEtcd(endpoints *[]string,)(etcd *Etcd,err error){
	etcd=&Etcd{}
	etcd.cli,err=client.New(client.Config{Endpoints: *endpoints,DialTimeout:2*time.Second})
	return
}
func (e *Etcd)LeaseGrant(ttl int64)(resp *client.LeaseGrantResponse,err error){
	return e.newLease().Grant(context.TODO(),ttl)
}
func (e *Etcd)KeepAlive(leaseId client.LeaseID)(resp <-chan *client.LeaseKeepAliveResponse,err error){
	return e.newLease().KeepAlive(context.TODO(),leaseId)
}
func (e *Etcd)KeepAliveOnce(leaseId client.LeaseID)(resp *client.LeaseKeepAliveResponse,err error){
	return e.newLease().KeepAliveOnce(context.TODO(),leaseId)
}
func (e *Etcd)Put(key, val string, opts ...client.OpOption)(*client.PutResponse, error){
	return e.newKV().Put(context.TODO(),key,val,opts...)
}
func (e *Etcd)Delete(key string, opts ...client.OpOption)(*client.DeleteResponse, error){
	return e.newKV().Delete(context.TODO(),key,opts...)
}
func (e *Etcd)Get(key string, opts ...client.OpOption)(*client.GetResponse,error){
	return e.newKV().Get(context.TODO(),key,opts...)
}
func (e *Etcd)Watch(key string,cb func(event *client.Event), opts ...client.OpOption){
	for resp:= range e.newWatcher().Watch(context.TODO(), key, opts...){
		for _,event:=range resp.Events{
			//todo 避免事件丢失
			go cb(event)
		}
	}
}
func (e *Etcd)newKV()client.KV{
	return client.NewKV(e.cli)
}
func (e *Etcd)newLease()client.Lease{
	return client.NewLease(e.cli)
}

func (e *Etcd)newWatcher()client.Watcher{
	return client.NewWatcher(e.cli)
}
func (e *Etcd)Shutdown()(err error){
	err=e.cli.Close()
	return
}