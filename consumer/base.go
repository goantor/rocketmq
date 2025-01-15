package consumer

import (
	"fmt"
	mq "github.com/apache/rocketmq-clients/golang/v5"
)

func newBaseClient(option IConsumerOption) (mq.SimpleConsumer, error) {
	p, err := mq.NewSimpleConsumer(
		option.TakeConfig(),
		option.TakeOptions()...)

	if err != nil {
		return nil, err
	}

	return p, nil
}

func testPrint() {
	fmt.Println("testPrint")
}
