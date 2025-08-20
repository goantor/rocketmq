package producer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var (
	config = Config{
		Endpoint:  "xxxx",
		AccessKey: "xxxx",
		SecretKey: "xxxx",
	}
)

type msgTest struct {
	Msg string `json:"msg"`
}

func TestNormalProducer(t *testing.T) {
	normalConfig := &DefaultProducerConfig{
		Config: config,
		Topic:  "test_normal_order",
		Kind:   TopicKindNormal,
	}
	p, err := NewNormalProducer(normalConfig)
	if err != nil {
		t.Fatal(err)
		return
	}

	msg := NewSendMessage(&msgTest{Msg: "ab"})
	msg.WithTag("ab")
	msg.WithKeys("ab1", "ab2")
	msg.WithProperty("ab3", "ab4")

	ret := p.Send(context.Background(), msg)
	if ret.TakeError() != nil {
		t.Fatal(ret.TakeError())
		return
	}

	for _, v := range ret.TakeReceipts() {
		t.Log(v.MessageID)
	}
}

func TestFifoProducer(t *testing.T) {
	fifoConfig := &DefaultProducerConfig{
		Config: config,
		Topic:  "test_force_order",
		Kind:   TopicKindFifo,
	}
	p, err := NewFifoProducer(fifoConfig)
	if err != nil {
		t.Fatal(err)
		return
	}

	msg := NewSendMessage(&msgTest{Msg: "one"})
	msg.WithKeys("one")
	msg.WithGroupKey("one")

	ret := p.Send(context.Background(), msg)
	if ret.TakeError() != nil {
		t.Fatal(ret.TakeError())
		return
	}

	for _, v := range ret.TakeReceipts() {
		t.Log(v.MessageID)
	}

	msg2 := NewSendMessage(&msgTest{Msg: "two"})
	msg2.WithKeys("two")
	msg2.WithGroupKey("twoooooooooo2222123123")

	ret2 := p.Send(context.Background(), msg2)
	if ret2.TakeError() != nil {
		t.Fatal(ret2.TakeError())
		return
	}

	for _, v := range ret2.TakeReceipts() {
		t.Log(v.MessageID)
	}
}

func TestDelayProducer(t *testing.T) {
	delayConfig := &DefaultProducerConfig{
		Config: config,
		Topic:  "test_delay",
		Kind:   TopicKindDelay,
	}

	p, err := NewDelayProducer(delayConfig)
	if err != nil {
		t.Fatal(err)
		return
	}

	msg := NewSendMessage(&msgTest{Msg: "ef"})
	msg.WithDelayDuration(time.Second * 10)
	msg.WithTag("ef")
	msg.WithKeys("ef1", "ef2")
	msg.WithProperty("ef3", "ef4")

	ret := p.Send(context.Background(), msg)
	if ret.TakeError() != nil {
		t.Fatal(ret.TakeError())
		return
	}

	for _, v := range ret.TakeReceipts() {
		t.Log(v.MessageID)
	}
}

func TestTransactionProducer(t *testing.T) {
	transactionConfig := &DefaultProducerConfig{
		Config: config,
		Topic:  "test_transaction",
		Kind:   TopicKindTransaction,
	}

	p, err := NewTransactionProducer(transactionConfig)
	if err != nil {
		t.Fatal(err)
		return
	}

	msg := NewSendMessage(&msgTest{Msg: "gh"})
	msg.WithTransactionHandle(commitHandle)
	msg.WithTag("gh")
	msg.WithKeys("gh1", "gh2")
	msg.WithProperty("gh3", "gh4")

	ret := p.Send(context.Background(), msg)
	if ret.TakeError() != nil {
		t.Fatalf("提交事务消息 err=%v", ret.TakeError())
		return
	}

	for _, v := range ret.TakeReceipts() {
		t.Logf("提交事务消息 %v", v.MessageID)
	}

	msg2 := &SendMessage{Body: []byte("回滚事务消息"), opts: &SendMessageOption{
		transactionHandle: rollbackHandle,
	}}
	ret2 := p.Send(context.Background(), msg2)
	if ret2.TakeError() == nil {
		t.Fatal("回滚事务消息错误")
		return
	}

	if ret2.TakeError().Error() != "rollback" {
		t.Fatalf("回滚事务消息错误 %v", ret.TakeError())
		return
	}

	t.Logf("回滚事务消息 %v", ret2.TakeError().Error())
}

func commitHandle(ctx context.Context) error {
	fmt.Println("commitHandle start----------------------------------")
	time.Sleep(3 * time.Second)
	fmt.Println("commitHandle end ----------------------------------")
	return nil
}

func rollbackHandle(ctx context.Context) error {
	time.Sleep(3 * time.Second)
	return errors.New("rollback")
}
