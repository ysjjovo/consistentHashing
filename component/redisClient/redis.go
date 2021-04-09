package redisClient

import (
	"github.com/mediocregopher/radix"
	"strconv"
	"sync"
)

type Redis struct{
	pool *radix.Pool
	conn radix.PubSubConn
	subChMap map[string]chan radix.PubSubMessage
	mu sync.RWMutex
}
func NewRedis(network, addr string, size int, opts ...radix.PoolOpt)(r *Redis,err error){
	r=&Redis{
		subChMap:make(map[string]chan radix.PubSubMessage,1),
	}
	r.pool, err = radix.NewPool(network, addr, size,opts...)
	if err != nil {
		return
	}
	conn, err := radix.Dial(network, addr)
	if err != nil {
		return
	}
	r.conn = radix.PubSub(conn)


	return r,nil
}
func(r *Redis)ZRange(key string,start,stop int,withScores bool,data *[]string) (err error) {
	args := []string{key, strconv.Itoa(start),strconv.Itoa(stop)}
	if withScores {
		args=append(args,"WITHSCORES")
	}
	err=r.pool.Do(radix.Cmd(data, "ZRANGE", args...))
	return
}
func(r *Redis)ZAdd(key string,scores []int,values []string) (err error) {
	args := []string{key}
	for i,v:=range scores{
		args=append(args,strconv.Itoa(v),values[i])
	}
	err=r.pool.Do(radix.Cmd(nil, "ZADD", args...))
	return
}
func(r *Redis)Del(key string) (err error) {
	err=r.pool.Do(radix.Cmd(nil, "DEL", key))
	return
}
func(r *Redis)ZCount(key string,min,max string) (err error) {
	err=r.pool.Do(radix.Cmd(nil, "ZCOUNT", key,min,max))
	return
}

func(r *Redis)Publish(channel,message string)error{
	return r.pool.Do(radix.Cmd(nil, "PUBLISH", channel, message))
}

func(r *Redis)Subscribe(channel string,cb func(*radix.PubSubMessage))(err error){
	subCh:=make(chan radix.PubSubMessage,1)
	r.mu.Lock()
    r.subChMap[channel]=subCh
	r.mu.Unlock()

	if err = r.conn.Subscribe(subCh, channel);err!=nil{
		return
	}
	go func() {
		for mesage:=range subCh{
			cb(&mesage)
		}
	}()
	return
}
func (r *Redis)Shutdown()(err error){
	r.mu.RLock()
	for _,v:=range r.subChMap{
		close(v)
	}
	r.mu.RUnlock()

	err=r.pool.Close()
	err1:=r.conn.Close()
	if err ==nil{
		err=err1
	}
	return
}
