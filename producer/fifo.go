package producer

import (
	"context"
	mq "github.com/apache/rocketmq-clients/golang/v5"
)

func NewFifoProducer(option IProducerOption) (*FifoProducer, error) {
	baseClient, _ := newBaseClient(option)
	return &FifoProducer{
		config: option,
		client: baseClient,
	}, nil
}

type FifoProducer struct {
	config IProducerOption
	client mq.Producer
}

func (f *FifoProducer) Send(ctx context.Context, message *SendMessage) *SendRet {
	msg := message.TakeMqMessage(f.config.TakeTopic())
	msg.SetMessageGroup("fifo")
	return NewSendRet(f.client.Send(ctx, msg))
}
