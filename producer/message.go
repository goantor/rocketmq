package producer

import (
	"context"
	"encoding/json"
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"time"
)

type TransactionHandle func(ctx context.Context) error

type Tag string // 消息标签

func NewSendMessage(val any) *SendMessage {
	body, _ := json.Marshal(val)
	return &SendMessage{
		Body: body,
		Opts: &SendMessageOption{},
	}
}

type SendMessageOption struct {
	Tag               Tag               `json:"-"`
	TransactionHandle TransactionHandle `json:"-"`
	DelayTime         time.Time         `json:"-"`
}

type SendMessage struct {
	Body []byte             `json:"body"`
	Opts *SendMessageOption `json:"-"`
}

func (s *SendMessage) WithTag(tag Tag) {
	s.Opts.Tag = tag
}

func (s *SendMessage) WithTransactionHandle(handle TransactionHandle) {
	s.Opts.TransactionHandle = handle
}

func (s *SendMessage) WithDelayTime(delayTime time.Time) {
	s.Opts.DelayTime = delayTime
}

func (s *SendMessage) WithDelayDuration(duration time.Duration) {
	s.Opts.DelayTime = time.Now().Add(duration)
}

func (s *SendMessage) TakeMqMessage(topic string) *mq.Message {
	msg := &mq.Message{
		Topic: topic,
		Body:  s.Body,
	}

	if s.Opts != nil {
		if s.Opts.Tag != "" {
			msg.SetTag(string(s.Opts.Tag))
		}
	}

	return msg
}

func NewSendRet(raw []*mq.SendReceipt, err error) *SendRet {
	return &SendRet{
		raw: raw,
		err: err,
	}
}

type SendRet struct {
	raw []*mq.SendReceipt
	err error
}

func (s *SendRet) TakeError() error {
	return s.err
}

func (s *SendRet) TakeReceipts() []*mq.SendReceipt {
	return s.raw
}
