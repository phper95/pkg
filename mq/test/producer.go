package main

import (
	"fmt"
	"gitee.com/phper95/pkg/mq"
	"github.com/Shopify/sarama"
)

const Topic = "test"

func main() {

	err := mq.InitAsyncKafkaProducer(mq.DefaultKafkaAsyncProducer, []string{"192.168.1.6:9091"}, nil)
	if err != nil {
		fmt.Println("InitAsyncKafkaProducer error", err)
	}
	err = mq.GetKafkaAsyncProducer(mq.DefaultKafkaAsyncProducer).Send(&sarama.ProducerMessage{Topic: Topic, Value: mq.KafkaMsgValueStrEncoder("test msg")})
	if err != nil {
		fmt.Println("Send msg error", err)
	} else {
		fmt.Println("Send msg success")
	}
}
