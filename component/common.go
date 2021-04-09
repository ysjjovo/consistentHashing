package component

import (
	"consistentHashing/component/etcd"
	"os"
)

var Etcd * etcd.Etcd

func InitComponents(){
	var err error
	if Etcd, err = etcd.NewEtcd(&[]string{"127.0.0.1:2379"});err!=nil{
		println(err.Error())
		os.Exit(1)
	}
	return
}