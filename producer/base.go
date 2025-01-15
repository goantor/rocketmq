package producer

import (
	"errors"
	mq "github.com/apache/rocketmq-clients/golang/v5"
)

var (
	ErrOption    = errors.New("option is err")
	ErrTopicKind = errors.New("topic kind is err")
)

func New(option IProducerOption) (IProducer, error) {
	if option == nil {
		return nil, ErrOption
	}

	switch option.TakeTopicKind() {
	case TopicKindNormal:
		return NewNormalProducer(option)
	case TopicKindDelay:
		return NewDelayProducer(option)
	case TopicKindFifo:
		return NewFifoProducer(option)
	case TopicKindTransaction:
		return NewTransactionProducer(option)
	}

	return nil, ErrTopicKind
}

func newBaseClient(option IProducerOption) (mq.Producer, error) {
	p, err := mq.NewProducer(
		option.TakeConfig(),
		option.TakeOptions()...)

	if err != nil {
		return nil, err
	}

	if err = p.Start(); err != nil {
		return nil, err
	}

	return p, nil
}
