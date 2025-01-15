package consumer

import (
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"time"
)

var (
	maxMessageNum int32 = 16
)

type DefaultConsumerConfig struct {
	ServerName       string
	Endpoint         string
	AccessKey        string
	SecretKey        string
	Group            string
	SubNums          int32
	MaxMsgNum        int32
	WaitSeconds      int32
	InvisibleSeconds int32
	Subscription     map[string]string
}

func (d *DefaultConsumerConfig) TakeConfig() *mq.Config {
	return &mq.Config{
		Endpoint: d.Endpoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    d.AccessKey,
			AccessSecret: d.SecretKey,
		},
		ConsumerGroup: d.Group,
	}
}

func (d *DefaultConsumerConfig) TakeTopics() []string {
	topics := make([]string, 0)
	for topic, _ := range d.Subscription {
		topics = append(topics, topic)
	}
	return topics
}

func (d *DefaultConsumerConfig) TakeGroup() string {
	return d.Group
}

func (d *DefaultConsumerConfig) TakeOptions() []mq.SimpleConsumerOption {
	opts := []mq.SimpleConsumerOption{
		mq.WithAwaitDuration(d.TakeWait()),
	}

	subscriptionExpress := make(map[string]*mq.FilterExpression)
	for topic, expression := range d.Subscription {
		subscriptionExpress[topic] = mq.NewFilterExpression(expression)
	}

	opts = append(opts, mq.WithSubscriptionExpressions(subscriptionExpress))
	return opts
}

func (d *DefaultConsumerConfig) TakeNum() int32 {
	if d.SubNums == 0 {
		return 1
	}
	return d.SubNums
}

func (d *DefaultConsumerConfig) TakeWait() time.Duration {
	if d.WaitSeconds == 0 {
		return 5 * time.Second
	}
	return time.Duration(d.WaitSeconds) * time.Second
}

func (d *DefaultConsumerConfig) TakeMaxMessageNum() int32 {
	if d.MaxMsgNum == 0 {
		return maxMessageNum
	}
	return d.MaxMsgNum
}

func (d *DefaultConsumerConfig) TakeInvisibleDuration() time.Duration {
	if d.InvisibleSeconds == 0 {
		return 20 * time.Second
	}

	return time.Duration(d.InvisibleSeconds) * time.Second
}

func (d *DefaultConsumerConfig) TakeUniqKey() string {
	return d.ServerName + "_" + d.Group
}
