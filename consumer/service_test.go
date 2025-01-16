package consumer

import (
	"fmt"
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/goantor/x"
	"os"
	"testing"
	"time"
)

func registerRouter(service *Service) {
	normalTopic := service.Topic("test_normal_order")
	{
		normalTopic.Register("ab", func(msg *Message) error {
			view := msg.View()
			fmt.Printf("normal tag:%s keys:%v properties:%v\n", *view.GetTag(), view.GetKeys(), view.GetProperties())
			return nil
		})
	}

	forceTopic := service.Topic("test_force_order")
	{
		forceTopic.Register("cd", func(msg *Message) error {
			view := msg.View()
			fmt.Printf("fifo tag:%s keys:%v properties:%v\n", *view.GetTag(), view.GetKeys(), view.GetProperties())
			return nil
		})
	}

	delayTopic := service.Topic("test_delay")
	{
		delayTopic.Register("ef", func(msg *Message) error {
			view := msg.View()
			fmt.Printf("delay tag:%s keys:%v properties:%v\n", *view.GetTag(), view.GetKeys(), view.GetProperties())
			return nil
		})
	}

	transactionTopic := service.Topic("test_transaction")
	{
		transactionTopic.Register("gh", func(msg *Message) error {
			view := msg.View()
			fmt.Printf("transaction tag:%s keys:%v properties:%v\n", *view.GetTag(), view.GetKeys(), view.GetProperties())
			return nil
		})
	}

}

func TestService(t *testing.T) {
	os.Setenv("mq.consoleAppender.enabled", "false")
	os.Setenv("rocketmq.client.logLevel", "error")

	mq.ResetLogger()
	normalConfig := &DefaultConsumerConfig{
		Endpoint:  "xxxx",
		AccessKey: "xxxx",
		SecretKey: "xxxx",
		Group:     "xxxx",
		SubNums:   1,
		MaxMsgNum: 5,
		Subscription: map[string]string{
			"test_normal_order": "ab",
			"test_force_order":  "cd",
			"test_delay":        "ef",
			"test_transaction":  "gh",
		},
	}
	service := NewService(x.NewContext(nil), normalConfig)
	registerRouter(service)
	err := service.Boot()
	if err != nil {
		t.Error(err)
	}

	time.Sleep(10 * time.Minute)
}
