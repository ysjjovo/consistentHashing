package redisClient

import (
	"consistentHashing/component/hashingCycle"
	"github.com/mediocregopher/radix"
	uuid "github.com/satori/go.uuid"
	"testing"
	"time"
)
const (
	channel="test"
	JobNum=5
)
func getRedis()(redis *Redis){
	var err error
	if redis,err= NewRedis("tcp", "127.0.0.1:6379", 1);err!=nil{
		println(err)
	}
	return
}
func TestDel(t *testing.T){
	if err := getRedis().Del("SORTED_JOBS");err!=nil{
		println(err.Error())
		return
	}
}
func TestInitJobs(t *testing.T){
	var sortedJobs []string
	var scores []int
	for i:=0;i<JobNum;i++{
		u,_ := uuid.NewV4()
		jobId := u.String()
		hash:= hashingCycle.Hash(jobId)
		scores=append(scores,hash)
		sortedJobs=append(sortedJobs,jobId)
	}

	if err:=getRedis().ZAdd("SORTED_JOBS",scores,sortedJobs);err!=nil{
		println(err.Error())
		return
	}
}
func TestZRange(t *testing.T){
	var data []string
	if err := getRedis().ZRange("SORTED_JOBS",0,-1,true,&data);err!=nil{
		println(err.Error())
		return
	}
}
func TestPubSub1(t *testing.T){
	redis:=getRedis()
	ticker := time.NewTicker(1 * time.Second)
	go func(){
		for{
			<-ticker.C
			redis.Publish("test","client1")
		}
	}()
	redis.Subscribe(channel, func(message *radix.PubSubMessage) {
		println("get message:",string(message.Message))
	})

	time.Sleep(5*time.Second)
	println("test close channel...")
	redis.Shutdown()
	println("sleeping...")
	time.Sleep(5*time.Second)
}

func TestPubSub2(t *testing.T){
	redis:=getRedis()
	ticker := time.NewTicker(1 * time.Second)
	go func(){
		for{
			<-ticker.C
			redis.Publish("test","client2")
		}
	}()
	redis.Subscribe(channel, func(message *radix.PubSubMessage) {
		println("get message:",string(message.Message))
	})

	time.Sleep(5*time.Second)
	println("test close channel...")
	redis.Shutdown()
	println("sleeping...")
	time.Sleep(5*time.Second)
}
