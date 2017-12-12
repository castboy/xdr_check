package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/optiopay/kafka"
	"github.com/widuu/goini"
)

var consumer kafka.Consumer
var broker kafka.Client

var err error

func InitBroker() {
	b := goini.SetConfig("conf.ini").GetValue("kafka", "broker")

	conf := kafka.NewBrokerConf("agent2")
	conf.AllowTopicCreation = false

	broker, err = kafka.Dial([]string{b + ":9092"}, conf)
	if err != nil {
		log.Fatal("can not connect kafka")
	}
}

func InitConsumer(topic string, partition int32, start int64) {
	conf := kafka.NewConsumerConf(topic, partition)
	if 0 == start {
		conf.StartOffset = LastOffset(topic, partition)
	} else {
		conf.StartOffset = start
	}

	conf.RetryLimit = 1
	consumer, err = broker.Consumer(conf)
	if nil != err {
		log.Fatal("init kafka consumer failed")
	}
}

func LastOffset(topic string, partition int32) int64 {
	start, _ := broker.OffsetLatest(topic, partition)
	return start - 1
}

func Consume(num int) {
	defer func() {
		if r := recover(); r != nil {
			log.Fatal("no data on kafka offset given")
		}
	}()
	for num > 0 {
		msg, err := consumer.Consume()
		if nil != err {
			log.Fatal("no data on kafka offset given")
		}

		fmt.Println(msg.Value)
		fmt.Println("")

		num--
	}

}

func main() {
	topic := flag.String("topic", "", "topic")
	partition := flag.Int("partition", 0, "partition")
	start := flag.Int("offset", 0, "offset")
	num := flag.Int("num", 1, "num")

	InitBroker()
	InitConsumer(*topic, int32(*partition), int64(*start))
	Consume(*num)
}
