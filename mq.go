package rocketmq

import (
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"os"
)

func ResetConfig() {
	// TODO
	os.Setenv("mq.consoleAppender.enabled", "false")
	os.Setenv("rocketmq.client.logLevel", "error")
	mq.ResetLogger()
}
