package model

//别忘了tag 否则映射不出来
type Config struct {
	KafkaConfig `ini:"kafka"`
	ESConfig `ini:"es"`
}
type KafkaConfig struct{
	Address string`ini:"address"`
	Topic string `ini:"topic"`

}
type ESConfig struct {
	Address string `ini:"address"`
	Index string `ini:"index"`
	MaxSize int `ini:"max_chan_size"`
	GoroutineNumber int `ini:"consumer_goroutine_num"`
}
