package main

import (
	"fmt"
	"logtransfer/es"
	"logtransfer/kafka"
	"logtransfer/model"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

//从kafka消费日志发往ES
func main(){
	//加载配置文件
	var cfg =new(model.Config)
	err:=ini.MapTo(cfg,"./config/logtranfer.ini")
	if err!=nil{
		logrus.Errorf("ini load failed err:%s ",err)
		return
	}
	fmt.Println("load ini config success")
	//	连接es   这个一定要在kafka初始化前边 因为信息管道在es中声明初始化，kafka用之前必须先初始化
	err=es.Init(cfg.ESConfig.Address,cfg.ESConfig.Index,cfg.ESConfig.MaxSize,cfg.ESConfig.GoroutineNumber)
	if err!=nil{
		logrus.Errorf("link es failed err:%s",err)
		return
	}
	fmt.Println("link es success!")
	//	连接kafka
	err=kafka.Init([]string{cfg.KafkaConfig.Address},cfg.KafkaConfig.Topic)
	if err!=nil{
		logrus.Errorf("link kafka failed err:%s",err)
		return
	}
	fmt.Println("link kafka success!")

	//让程序卡住  使用select是程序卡住  不占用cpu
	select {}


}
