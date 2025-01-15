package producer

import (
	"context"
	mq "github.com/apache/rocketmq-clients/golang/v5"
)

func NewNormalProducer(option IProducerOption) (*NormalProducer, error) {
	baseClient, _ := newBaseClient(option)
	return &NormalProducer{
		config: option,
		client: baseClient,
	}, nil
}

type NormalProducer struct {
	config IProducerOption
	client mq.Producer
}

func (n *NormalProducer) Send(ctx context.Context, message *SendMessage) *SendRet {
	msg := message.TakeMqMessage(n.config.TakeTopic())
	return NewSendRet(n.client.Send(ctx, msg))
}
