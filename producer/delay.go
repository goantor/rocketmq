package producer

import (
	"context"
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"time"
)

func NewDelayProducer(option IProducerOption) (*DelayProducer, error) {
	baseClient, _ := newBaseClient(option)
	return &DelayProducer{
		config: option,
		client: baseClient,
	}, nil
}

type DelayProducer struct {
	config IProducerOption
	client mq.Producer
}

func (d *DelayProducer) Send(ctx context.Context, message *SendMessage) *SendRet {
	msg := message.TakeMqMessage(d.config.TakeTopic())
	var delayTime time.Time
	if message.Opts == nil || message.Opts.DelayTime.IsZero() {
		delayTime = time.Now()
	}
	msg.SetDelayTimestamp(delayTime)
	return NewSendRet(d.client.Send(ctx, msg))
}
