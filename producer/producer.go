package producer

import (
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/bitly/go-nsq"

	"hotpu.cn/xkefu/common/config"
	"hotpu.cn/xkefu/common/log"
	"hotpu.cn/xkefu/common/queue/nsq/common"
)

var (
	haProducder *HaProducer
)

func Get() (*HaProducer, error) {
	if haProducder != nil {
		return haProducder, nil
	}
	return nil, config.ErrHaProducerNotInitialized
}

func InitFromConfig(nsqTcpAddrs []string) error {
	log.Info("common.nsq.producer", "InitFromConfig", "run...")

	if len(nsqTcpAddrs) == 0 {
		return config.ErrNsqTcpAddrs
	}

	p, err := NewHaProducer(nsqTcpAddrs, common.GetDefaultNsqConfig())
	if err != nil {
		return err
	}

	// Check the status of the producers and take a tolerant strategy.
	status, err := p.Ping()
	if err != nil {
		log.Error("common.nsq.producer", "InitFromConfig", "producer.InitFromConfig > Error when ping the producer: %s", err.Error())
	}

	if status.OkRatio() < 0.5 {
		return fmt.Errorf("More than half of the producer not usable: %s", strings.Join(status.NotOk, ","))
	}
	if len(status.NotOk) > 0 {
		log.Error("common.nsq.producer", "InitFromConfig", "producer.InitFromConfig > Some producers are not usable, please check: %s", strings.Join(status.NotOk, ","))
	}

	haProducder = p
	return nil
}

type HaProducer struct {
	NsqdAddrs []string
	producers []*nsq.Producer
	nsqConfig nsq.Config // the config used to configure the nsq producer connection
}

func NewHaProducer(addrs []string, config *nsq.Config) (*HaProducer, error) {
	hp := &HaProducer{
		NsqdAddrs: addrs,
		producers: make([]*nsq.Producer, len(addrs)),
	}

	config.WriteTimeout = time.Second * 3 //
	config.DialTimeout = time.Second * 5  // the defult value 1s tend to timeout when pinging

	log.Debug("common.nsq.producer", "NewHaProducer", "The nsq config used: %v", config)

	for k, addr := range addrs {
		p, err := nsq.NewProducer(addr, config)
		if err != nil {
			return nil, err
		}
		hp.producers[k] = p
	}

	return hp, nil
}

type PingStatus struct {
	totalCnt int
	Ok       []string
	NotOk    []string
}

func (s PingStatus) OkRatio() float64 {
	if s.totalCnt == 0 {
		return 0
	}
	return float64(len(s.Ok)) / float64(s.totalCnt)
}

type PingResult struct {
	Producer string // The producer address
	Err      error  // If there is error
}

// Do a cocurrent Ping action on all producers.
// It returns a brief status, and returns the first met error.
func (p *HaProducer) Ping() (PingStatus, error) {
	out := make(chan PingResult, 10) // prevent the send action being blocked
	var err error
	var status = PingStatus{totalCnt: len(p.producers)}

	for _, p := range p.producers {
		go func(p *nsq.Producer) {
			log.Debug("common.nsq.producer", "HaProducer.Ping", "Ping the producer: %s", p.String())

			// If the DialTimeout is 5 seconds, then the whole dial timeout time is 5 * 5 = 25s
			for i := 0; i < 5; i++ {
				if err := p.Ping(); err != nil {
					// the default DialTimeout is nsq config is 1s, this value should be
					// increased in developing phase, in which case the nsq node is deployed
					// on a remote machine
					if strings.Contains(err.Error(), "i/o timeout") && i != 4 {
						continue
					}
					log.Error("common.nsq.producer", "HaProducer.Ping", "Error when ping producer %s: %s", p.String(), err.Error())
					out <- PingResult{p.String(), err}
					break
				} else {
					log.Info("common.nsq.producer", "HaProducer.Ping", "Ping success")
					out <- PingResult{p.String(), nil}
					return
				}
			}
		}(p)
	}

	for i := 0; i < len(p.producers); i++ {
		select {
		case v := <-out:
			if v.Err != nil {
				if err == nil { // only store the first met error
					err = v.Err
				}
				status.NotOk = append(status.NotOk, v.Producer)
			} else {
				status.Ok = append(status.Ok, v.Producer)
			}
		}
	}

	return status, err
}

func (p *HaProducer) Publish(topic string, body []byte) error {
	log.Debug("common.nsq.producer", "HaProducer.Publish", "run...")
	log.Debug("common.nsq.producer", "HaProducer.Publish", "The message: %s", body)

	// retry := 2

	// @TODO: Distribute evenly??
	// @TODO: Use hash to distribute and make it more sticker?

	var err error // used to store the last error

	h := fnv.New32a()
	_, err = h.Write([]byte(topic))
	if err != nil {
		log.Error("common.nsq.producer", "HaProducer.Publish", "can't hash topic:%s", err.Error())
		return err
	}
	v := int(h.Sum32())
	l := len(p.producers)
	i := v % l
	for try := 0; try < len(p.producers); try++ {
		produer := p.producers[i]
		nsqdAddr := p.NsqdAddrs[i]
		if err = produer.Publish(topic, body); err != nil {
			log.Error("common.nsq.producer", "HaProducer.Publish", "%d try:Can't publish message to nsqd at %s: %s", try+1, nsqdAddr, err.Error())
			i++
			continue
		} else {
			return nil
		}
	}

	return err

	// retryLoop:
	// 	for i := 0; i < retry; i++ {
	// 		for _, prod := range p.producers {
	// 			if e := prod.Publish(topic, body); e != nil {
	// 				err := e
	// 				log.Error("common.nsq.producer", "HaProducer.Publish", "Can't publish message: %s", err.Error())
	// 				continue
	// 			}

	// 			// once published once, try no more
	// 			break retryLoop
	// 		}
	// 	}

	// if err != nil {
	// 	return err
	// }

	// return nil
}
