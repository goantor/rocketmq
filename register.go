package rocketmq

import (
	"github.com/goantor/rocketmq/producer"
	"sync"
)

type ProducerRegister interface {
	Register(uniq string, producer producer.IProducer)
	Take(topic string) (producer.IProducer, bool)
}

func NewProducerRegistry() ProducerRegister {
	return &ProducerRegistry{
		container: make(map[string]producer.IProducer),
	}
}

type ProducerRegistry struct {
	mux       sync.RWMutex
	container map[string]producer.IProducer
}

func (p *ProducerRegistry) Register(uniq string, producer producer.IProducer) {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.container[uniq] = producer
}

func (p *ProducerRegistry) Take(uniq string) (producer producer.IProducer, exists bool) {
	p.mux.RLock()
	defer p.mux.RUnlock()

	producer, exists = p.container[uniq]
	return
}
