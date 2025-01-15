package consumer

import (
	"context"
	"errors"
	"fmt"
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/goantor/x"
	"time"
)

func NewSimpleConsumer(config IConsumerOption) (*SimpleConsumer, error) {
	baseClient, err := newBaseClient(config)
	if err != nil {
		return nil, err
	}
	return &SimpleConsumer{
		config: config,
		client: baseClient,
	}, nil
}

type SimpleConsumer struct {
	config IConsumerOption
	client mq.SimpleConsumer
}

func (s *SimpleConsumer) Start() error {
	return s.client.Start()
}

func (s *SimpleConsumer) Stop() error {
	return s.client.GracefulStop()
}

func (s *SimpleConsumer) Listen(ctx x.Context, router *router) {
	for i := 0; i < int(s.config.TakeNum()); i++ {
		go s.Receive(ctx, s.client, router)
	}
}

func (s *SimpleConsumer) Receive(ctx x.Context, consumer mq.SimpleConsumer, router *router) {
	for {
		mvs, err := consumer.Receive(context.Background(), s.config.TakeMaxMessageNum(), s.config.TakeInvisibleDuration())

		if err != nil {
			var status *mq.ErrRpcStatus
			if errors.As(err, &status) {
				if status.GetCode() == 40401 {
					time.Sleep(2 * time.Second)
					continue
				}
			}

			time.Sleep(2 * time.Second)
			continue
		}

		for _, view := range mvs {
			handler, ok := router.takeHandler(view)
			if handler == nil || !ok {
				ctx.Warn("consumer no handler", x.H{
					"msg_id": view.GetMessageId(),
					"topic":  view.GetTopic(),
					"group":  s.config.TakeGroup(),
				})
				continue
			}
			go s.handleMsg(ctx, consumer, view, handler)
		}
	}
}

func (s *SimpleConsumer) handleMsg(ctx x.Context, consumer mq.SimpleConsumer, view *mq.MessageView, handler Handler) {
	topic := view.GetTopic()
	defer func() {
		if err := recover(); err != nil {
			newError := errors.New(fmt.Sprintf("%v", err))
			ctx.Error("consume handle panic", newError, x.H{
				"topic":  topic,
				"msg_id": view.GetMessageId(),
			})
		}
	}()
	err := handler(NewMsg(ctx, view))
	if err != nil {
		ctx.Error("topic handle error", err, x.H{
			"topic":  topic,
			"msg_id": view.GetMessageId(),
		})
		// TODO 是否 不ACK
	}
	_ = consumer.Ack(context.Background(), view)
}
