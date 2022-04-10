package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"logtransfer/es"
)
//初始化kafka
//从中取数据放在通道中
func Init(addr []string,topic string)(err error){
	consumer,err:=sarama.NewConsumer(addr,nil)
	if err!=nil{
		fmt.Println("creater consumer failed err:",err)
		return
	}
	//拿到topic下面的分区
	partitionList ,err :=consumer.Partitions(topic)
	if err!=nil{
		fmt.Println("partitionList get failed err:",err)
		return
	}
	for partition :=range partitionList{
		pc,err:=consumer.ConsumePartition(topic,int32(partition),sarama.OffsetNewest)
		if err!=nil{
			fmt.Printf("failed to start consumer for partition %d ,err: %v:",partition,err)
			return err
		}
		//这里如果有defer  携程会在这个函数结束以后被强制关闭
		//defer pc.AsyncClose()
		//异步读分区数据
		go func(sarama.PartitionConsumer){
			for msg:=range pc.Messages(){
				//为了将同步流程异步化  将取出的日志放在通道中
				//fmt.Println(msg.Topic,msg.Value)
				var m1 map[string]interface{}
				err=json.Unmarshal(msg.Value,&m1)
				if err!=nil{
					logrus.Error("kafka info msg unmarshal failed err:",err)
					continue
				}
				es.PutLogData(m1)
			}
		}(pc)
	}
	return
}
