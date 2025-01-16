package producer

import (
	"context"
	"errors"
	mq "github.com/apache/rocketmq-clients/golang/v5"
)

var (
	ErrTransactionNoHandle = errors.New("transaction no handle")
)

func NewTransactionProducer(option IProducerOption) (*TransactionProducer, error) {
	baseClient, _ := newBaseClient(option)
	return &TransactionProducer{
		config: option,
		client: baseClient,
	}, nil
}

type TransactionProducer struct {
	config IProducerOption
	client mq.Producer
}

func (t *TransactionProducer) Send(ctx context.Context, message *SendMessage) *SendRet {
	msg := message.TakeMqMessage(t.config.TakeTopic())

	transaction := t.client.BeginTransaction()
	resp, err := t.client.SendWithTransaction(ctx, msg, transaction)
	if err != nil {
		return NewSendRet(nil, err)
	}

	handler := message.TakeTransactionHandle()
	if handler == nil {
		_ = transaction.RollBack()
		return NewSendRet(nil, ErrTransactionNoHandle)
	}

	err = handler(ctx)

	if err != nil {
		_ = transaction.RollBack()
		return NewSendRet(nil, err)
	}

	_ = transaction.Commit()
	return NewSendRet(resp, nil)
}
