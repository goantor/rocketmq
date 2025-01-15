package producer

import (
	"context"
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"time"
)

type IProducer interface {
	Send(ctx context.Context, message *SendMessage) *SendRet
}

type IOption interface {
	TakeConfig() *mq.Config
}

type IProducerOption interface {
	IOption
	TakeUniqKey() string
	TakeTopic() string
	TakeOptions() []mq.ProducerOption
	TakeTopicKind() TopicKind
}

type IConsumerOption interface {
	IOption
	TakeTopics() []string
	TakeGroup() string
	TakeOptions() []mq.SimpleConsumerOption
	TakeNum() int
	TakeWait() time.Duration
	TakeMaxMessageNum() int32
	TakeInvisibleDuration() time.Duration
}
