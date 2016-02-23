package common

import (
	"fmt"

	"hotpu.cn/xkefu/common/consistent"
)

var (
	topic *Topic
)

type Topic struct {
	*consistent.Circle
}

func init() {
	topic = &Topic{
		consistent.New(),
	}

	topic.Set([]string{
		"1",
		"2",
		"3",
		"4",
	})
}

func MessageProducerTopicGet(serviceID int64, key string) (string, error) {
	elt, err := topic.Get(key)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("message.%d.%s", serviceID, elt), nil
}

func MessageConsumerTopicGet(serviceID int64, key string) string {
	return fmt.Sprintf("message.%d.%s", serviceID, key)
}

func Members() []string {
	return topic.Members()
}
