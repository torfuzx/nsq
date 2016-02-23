package consumer

import (
	"io/ioutil"
	l "log"

	n "github.com/bitly/go-nsq"
)

// ConsumerBasic is supposed just the same as the original consumer, with a
// a little tweak.
type ConsumerBasic struct {
	*n.Consumer
}

func NewConsumerBasic(topic string, channel string, config *n.Config, logLevel n.LogLevel) (*ConsumerBasic, error) {
	c, err := n.NewConsumer(topic, channel, config)
	if err != nil {
		return nil, err
	}
	consumer := &ConsumerBasic{c}

	// Set the logger properly
	var nullLogger = l.New(ioutil.Discard, "", l.LstdFlags)
	switch logLevel {
	case n.LogLevelDebug:
		consumer.SetLogger(&NsqDebugLogger{}, n.LogLevelDebug)
		consumer.SetLogger(&NsqInfoLogger{}, n.LogLevelInfo)
		consumer.SetLogger(&NsqWarningLogger{}, n.LogLevelWarning)
		consumer.SetLogger(&NsqErrorLogger{}, n.LogLevelError)

	case n.LogLevelInfo:
		consumer.SetLogger(nullLogger, n.LogLevelDebug)
		consumer.SetLogger(&NsqInfoLogger{}, n.LogLevelInfo)
		consumer.SetLogger(&NsqWarningLogger{}, n.LogLevelWarning)
		consumer.SetLogger(&NsqErrorLogger{}, n.LogLevelError)

	case n.LogLevelWarning:
		consumer.SetLogger(nullLogger, n.LogLevelDebug)
		consumer.SetLogger(nullLogger, n.LogLevelInfo)
		consumer.SetLogger(&NsqWarningLogger{}, n.LogLevelWarning)
		consumer.SetLogger(&NsqErrorLogger{}, n.LogLevelError)

	case n.LogLevelError:
		consumer.SetLogger(nullLogger, n.LogLevelDebug)
		consumer.SetLogger(nullLogger, n.LogLevelInfo)
		consumer.SetLogger(nullLogger, n.LogLevelWarning)
		consumer.SetLogger(&NsqErrorLogger{}, n.LogLevelError)
	}

	return consumer, err
}
