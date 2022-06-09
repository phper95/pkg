package main

import (
	"encoding/json"
	"fmt"
	"gitee.com/phper95/pkg/logger"
	"gitee.com/phper95/pkg/mq"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"time"
)

var (
	hosts = []string{"192.168.1.6:9091"}
	topic = "test"
)

func main() {
	produceAsyncMsg()
	//produceSyncMsg()
	//consumeMsg()
}

type Msg struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	CreateAt int64  `json:"create_at"`
}

func produceAsyncMsg() {
	err := mq.InitAsyncKafkaProducer(mq.DefaultKafkaAsyncProducer, hosts, nil)
	if err != nil {
		fmt.Println("InitAsyncKafkaProducer error", err)
	}
	msg := Msg{
		ID:       1,
		Name:     "test name async",
		CreateAt: time.Now().Unix(),
	}
	msgBody, _ := json.Marshal(msg)

	err = mq.GetKafkaAsyncProducer(mq.DefaultKafkaAsyncProducer).Send(&sarama.ProducerMessage{Topic: topic, Value: mq.KafkaMsgValueEncoder(msgBody)})
	if err != nil {
		fmt.Println("Send msg error", err)
	} else {
		fmt.Println("Send msg success")
	}
	//异步提交需要等待
	time.Sleep(3 * time.Second)
}
func produceSyncMsg() {
	err := mq.InitSyncKafkaProducer(mq.DefaultKafkaSyncProducer, hosts, nil)
	if err != nil {
		fmt.Println("InitSyncKafkaProducer error", err)
	}

	msg := Msg{
		ID:       2,
		Name:     "test name sync",
		CreateAt: time.Now().Unix(),
	}
	msgBody, _ := json.Marshal(msg)
	partion, offset, err := mq.GetKafkaSyncProducer(mq.DefaultKafkaSyncProducer).Send(&sarama.ProducerMessage{Topic: topic, Value: mq.KafkaMsgValueEncoder(msgBody)})
	if err != nil {
		fmt.Println("Send msg error", err)
	} else {
		fmt.Println("Send msg success partion ", partion, "offset", offset)
	}
}

func consumeMsg() {
	_, err := mq.StartKafkaConsumer(hosts, []string{topic}, "test-group", nil, msgHandler)
	if err != nil {
		fmt.Println(err)
	}
	select {}
}

func msgHandler(message *sarama.ConsumerMessage) (bool, error) {
	fmt.Println("消费消息:", "topic:", message.Topic, "Partition:", message.Partition, "Offset:", message.Offset, "value:", string(message.Value))
	msg := Msg{}
	err := json.Unmarshal(message.Value, &msg)
	if err != nil {
		//解析不了的消息怎么处理？
		logger.Error("Unmarshal error", zap.Error(err))
		return true, nil
	}
	fmt.Println("msg : ", msg)
	return true, nil
}
