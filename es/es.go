package es

import(
	"fmt"
	"github.com/olivere/elastic/v7"
	"context"
	"github.com/sirupsen/logrus"
)


type ESClient struct {
	client *elastic.Client
	index string
	LogDataChan chan interface{}//接收日志的channel
}
var (
	esClient= new(ESClient)//这里必须是new初始化一个地址会有字段，直接用*会报错因为是空指针
)
//初始化Es
func Init(addr string,index string,maxSize,goroutineNum int)(err error){

	client, err := elastic.NewClient(elastic.SetURL("http://"+addr))
	if err != nil {
		// Handle error
		panic(err)
	}
	esClient.client=client
	esClient.index=index
	esClient.LogDataChan=make(chan interface{},maxSize)

	fmt.Println("connect to es success")
	//从通道取数据放到es中
	for i:=0;i<goroutineNum;i++{
		go sendToEs()

	}

	return
}
func sendToEs(){

	for msg:=range esClient.LogDataChan{
		put1,err :=esClient.client.Index().Index(esClient.index).BodyJson(msg).Do(context.Background())

		if err!=nil{
			logrus.Error("msg send to es failed err:",err)
			continue
		}
		fmt.Println(put1.Type,put1.Result)
	}
}
//将一个首字母大写的函数 从包外接收数据
func PutLogData(msg interface{}){
	esClient.LogDataChan<-msg
}