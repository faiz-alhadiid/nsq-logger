package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
)

func main() {
	nsqlookupd := flag.String("nsqlookupd", "localhost:4161", "NSQLOOKUPD Address")
	topic := flag.String("topic", "", "Topic Name")
	channel := flag.String("channel", "test", "Channel Name")
	flag.Parse()

	if *topic == "" {
		log.Fatal("topic cannot be empty")
	}
	consumer, err := nsq.NewConsumer(*topic, *channel, nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("NSQ-Logger Running on nsqlookupd %v topic %v and channel%v\n", *nsqlookupd, *topic, *channel)
	consumer.AddHandler(nsq.HandlerFunc(logMessageBody))

	if err = consumer.ConnectToNSQLookupd(*nsqlookupd); err != nil {
		log.Fatal(err)
	}
}

func logMessageBody(nsq *nsq.Message) error {
	fmt.Println("New Message at:", time.Unix(0, nsq.Timestamp))
	b := nsq.Body
	indent, err := json.MarshalIndent(json.RawMessage(b), "", "    ")
	if err != nil {
		fmt.Println(indent)
	} else {
		fmt.Println(b)
	}
	return nil
}
