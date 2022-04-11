package main

import (
	"fmt"
	"logtransfer/es"
	"logtransfer/influxDB"
	"logtransfer/kafka"
	"logtransfer/model"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"logtransfer/sendToinflux"
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
	consum,err:=kafka.Init([]string{cfg.KafkaConfig.Address})
	if err!=nil{
		logrus.Errorf("link kafka  failed err:%s",err)
		return
	}
	//	发送日志信息到es中的管道中
	go kafka.ToESChan(consum,cfg.KafkaConfig.Topic[0])
	if err!=nil{
		logrus.Errorf("link kafka get log info failed err:%s",err)
		return
	}
	//初始化influx
	err = influxDB.InitInfluxDB()
	if err != nil {
		logrus.Errorf("influxDB init failed err:%s", err)
		return
	}
	//连接kafka 消费者去取性能信息，发往influxDb的管道中
	go kafka.ToinfluxChan(consum,cfg.KafkaConfig.Topic[1])
	if err != nil {
		logrus.Errorf("get sex_able_info from kafka failed err:%s", err)
		return
	}
	sendToinflux.SendMsgToDB()
	if err!=nil{
		logrus.Errorf("link kafka get sexAble info failed err:%s",err)
		return
	}
	fmt.Println("link kafka success!")

	//让程序卡住  使用select是程序卡住  不占用cpu
	select {}


}
