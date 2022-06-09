package main

import (
	"fmt"
	"gitee.com/phper95/pkg/mq"
	"github.com/Shopify/sarama"
	"time"
)

var (
	hosts = []string{"192.168.1.6:9091"}
	topic = "test"
)

func main() {
	produceAsyncMsg()
	//produceSyncMsg()
	consumeMsg()
}

func produceAsyncMsg() {
	err := mq.InitAsyncKafkaProducer(mq.DefaultKafkaAsyncProducer, hosts, nil)
	if err != nil {
		fmt.Println("InitAsyncKafkaProducer error", err)
	}
	err = mq.GetKafkaAsyncProducer(mq.DefaultKafkaAsyncProducer).Send(&sarama.ProducerMessage{Topic: topic, Value: mq.KafkaMsgValueEncoder([]byte("test msg"))})
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
	_, _, err = mq.GetKafkaSyncProducer(mq.DefaultKafkaSyncProducer).Send(&sarama.ProducerMessage{Topic: topic, Value: mq.KafkaMsgValueStrEncoder("test msg")})
	if err != nil {
		fmt.Println("Send msg error", err)
	} else {
		fmt.Println("Send msg success")
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
	return true, nil
}
