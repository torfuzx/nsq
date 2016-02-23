package common

import (
	"fmt"

	"github.com/bitly/go-nsq"
)

func GetKey(topic, channel string) string {
	return fmt.Sprintf("%s_%s", topic, channel)
}

func GetDefaultNsqConfig() *nsq.Config {
	return nsq.NewConfig()
}
