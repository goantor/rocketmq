package producer

import (
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

type TopicKind string

const (
	TopicKindNormal      TopicKind = "NORMAL"
	TopicKindFifo        TopicKind = "FIFO"
	TopicKindDelay       TopicKind = "DELAY"
	TopicKindTransaction TopicKind = "Transactional"
)

type Config struct {
	ServerName string
	Endpoint   string
	AccessKey  string
	SecretKey  string
}

type DefaultProducerConfig struct {
	Config
	Topic string
	Kind  TopicKind
}

func (p *DefaultProducerConfig) TakeConfig() *mq.Config {
	return &mq.Config{
		Endpoint: p.Endpoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    p.AccessKey,
			AccessSecret: p.SecretKey,
		},
	}
}

func (p *DefaultProducerConfig) TakeTopic() string {
	return p.Topic
}

func (p *DefaultProducerConfig) TakeOptions() []mq.ProducerOption {
	opts := []mq.ProducerOption{
		mq.WithTopics(p.Topic),
	}

	if p.Kind == TopicKindTransaction {
		opts = append(opts, mq.WithTransactionChecker(&mq.TransactionChecker{
			Check: func(msg *mq.MessageView) mq.TransactionResolution {
				return mq.COMMIT
			},
		}))
	}

	return opts
}

func (p *DefaultProducerConfig) TakeUniqKey() string {
	return p.ServerName + "_" + p.Topic
}

func (p *DefaultProducerConfig) TakeTopicKind() TopicKind {
	return p.Kind
}
