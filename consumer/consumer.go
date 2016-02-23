package consumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/bitly/go-nsq"

	"hotpu.cn/xkefu/common/config"
	"hotpu.cn/xkefu/common/log"
	"hotpu.cn/xkefu/common/queue/nsq/common"
)

var (
	consumers     = make(map[string]*Consumer, 4)
	mutex         = &sync.RWMutex{}
	defaultConfig = nsq.NewConfig()
)

type Consumer struct {
	Topic   string
	Channel string
	*ConsumerBasic
}

// InitFromConfig init a consumer for specific topic+channel
func InitFromConfig(serviceID, topic, channel string) (*Consumer, error) {
	log.Info("common.nsq.consumer", "InitFromConfig", "consumer.InitFromConfig run...")

	mutex.Lock()
	defer mutex.Unlock()

	// @todo: Expose settings to the config file

	nsqConfig := defaultConfig
	nsqConfig.ClientID = fmt.Sprintf("%s.%s.%s", serviceID, topic, channel)
	nsqConfig.ReadTimeout = time.Second * 4
	nsqConfig.HeartbeatInterval = time.Second * 2

	consumerBasic, err := NewConsumerBasic(topic, channel, defaultConfig, nsq.LogLevelDebug)
	consumerBasic.SetLogger(NsqDebugLogger{}, nsq.LogLevelDebug)
	consumerBasic.ChangeMaxInFlight(2)

	if err != nil {
		return nil, err
	}

	key := common.GetKey(topic, channel)

	if _, ok := consumers[key]; ok { // already initialized
		return nil, config.ErrConsumerInitialized
	}

	consumer := &Consumer{
		Topic:         topic,
		Channel:       channel,
		ConsumerBasic: consumerBasic,
	}
	consumers[key] = consumer
	return consumer, nil
}

// GetConsumer get a consumer for specific topci+channel
func Get(topic, channel string) (*Consumer, error) {
	mutex.RLock()
	defer mutex.RUnlock()

	key := common.GetKey(topic, channel)
	v, ok := consumers[key]
	if ok {
		return v, nil
	}
	return nil, nil
}

func (c *Consumer) AddHandler(handler nsq.Handler, concurrency int) {
	c.AddConcurrentHandlers(handler, concurrency)
}

// Start starts a consumer
func (c *Consumer) Start(lookupdAddrs []string) error {
	count := 0

retryLoop:
	count++
	log.Info("common.nsq.consumer", "Consumer.Start", "Run for the %d time", count)

	if len(lookupdAddrs) == 0 {
		return config.ErrLookupdAddrs
	}

	// poll the nsqlookupd for new producer
	log.Info("common.nsq.consumer", "Consumer.Start", "ConnectToNSQLookupds start")
	if err := c.ConsumerBasic.ConnectToNSQLookupds(lookupdAddrs); err != nil {
		log.Critical("common.nsq.consumer", "Consumer.Start", "Can't connect to nsqlookupd hosts: %s", err.Error())
	}
	log.Info("common.nsq.consumer", "Consumer.Start", "ConnectToNSQLookupds success")

	timeTicker := time.NewTicker(time.Second * 2)

	for {
		log.Debug("common.nsq.consumer", "Consumer.Start", "looping")

		select {
		case <-c.ConsumerBasic.StopChan:
			goto retryLoop

		case <-timeTicker.C:
			log.Debug("common.nsq.consumer", "Consumer.Start", "The stats: %+v", c.ConsumerBasic.Stats())
			c.ConsumerBasic.ChangeMaxInFlight(2)

			stats := c.ConsumerBasic.Stats()

			if stats.MessagesReceived-stats.MessagesFinished > 0 {
				log.Error("common.nsq.consumer", "Consumer.Start", "Something is wrong. Retrying.")
				c.ConsumerBasic.Stop()
				goto retryLoop
			}
		}
	}

	log.Error("common.nsq.consumer", "Consumer.Start", "The consumer stops.")

	return nil
}
