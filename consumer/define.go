package consumer

import (
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"time"
)

type Handler func(msg *Message) error

type IOption interface {
	TakeConfig() *mq.Config
}

type IConsumerOption interface {
	IOption
	TakeTopics() []string
	TakeGroup() string
	TakeUniqKey() string
	TakeOptions() []mq.SimpleConsumerOption
	TakeNum() int32
	TakeWait() time.Duration
	TakeMaxMessageNum() int32
	TakeInvisibleDuration() time.Duration
}
