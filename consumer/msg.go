package consumer

import (
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/goantor/x"
)

func NewMsg(ctx x.Context, view *mq.MessageView) *Message {
	return &Message{
		ctx:  ctx,
		view: view,
	}
}

type Message struct {
	ctx  x.Context
	view *mq.MessageView
}

func (m *Message) Context() x.Context {
	return m.ctx
}

func (m *Message) View() *mq.MessageView {
	return m.view
}
