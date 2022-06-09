package mq

import (
	"gitee.com/phper95/pkg/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/eapache/go-resiliency/breaker"
)

const (
	//KafkaConsumerConnected 消费者已连接
	KafkaConsumerConnected string = "connected"
	//KafkaConsumerDisconnected 消费者断开
	KafkaConsumerDisconnected string = "disconnected"
)

type Consumer struct {
	hosts    []string
	topics   []string
	config   *cluster.Config
	consumer *cluster.Consumer
	status   string
	groupID  string

	breaker    *breaker.Breaker
	reConnect  chan bool
	statusLock sync.Mutex
	exit       bool
}

//KafkaMessageHandler  消费者回调函数
type KafkaMessageHandler func(message *sarama.ConsumerMessage) (bool, error)

//kafka 消费者配置
func getKafkaDefaultConsumerConfig() (config *cluster.Config) {
	config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	config.Consumer.Fetch.Default = 1024 * 1024 * 2
	//config.Consumer.MaxProcessingTime = 100        //消息入队时间
	//config.Consumer. = 100        //消息处理时间，超时则reblance给其他消费者，kafka默认值为300000，配置项为max.poll.interval.ms

	//config.Consumer.Group.Session.Timeout = 550000    //消费者是否存活的心跳检测，默认是10秒，对应kafka session.timeout.ms配置
	//config.Consumer.Group.Heartbeat.Interval = 100000 //消费者协调器心跳间隔时间，默认3s此值设置不超过group session超时时间的三分之一
	//config.Consumer.Group.Rebalance.Timeout = 3600 //此配置是重新平衡时消费者加入group的超时时间，默认是60s
	config.Version = sarama.V2_0_0_0
	return
}

//启动消费者
func StartKafkaConsumer(hosts, topics []string, groupID string, config *cluster.Config, f KafkaMessageHandler) (*Consumer, error) {
	var err error
	if config == nil {
		config = getKafkaDefaultConsumerConfig()
	}
	consumer := &Consumer{
		hosts:   hosts,
		config:  config,
		status:  KafkaConsumerDisconnected,
		groupID: groupID,
		topics:  topics,

		breaker:   breaker.New(3, 1, 3*time.Second),
		reConnect: make(chan bool),
		exit:      false,
	}

	if consumer.consumer, err = cluster.NewConsumer(hosts, groupID, topics, consumer.config); err != nil {
		return consumer, err
	} else {
		consumer.status = KafkaConsumerConnected
		logger.Info("kafka consumer started", zap.Any(groupID, topics))
	}
	go consumer.keepConnect()
	go consumer.consumerMessage(f)

	return consumer, err
}

// Exit 退出消费
func (c *Consumer) Close() error {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	c.exit = true
	return c.consumer.Close()

}

//检查kafka连接状态,如果断开链接则尝试重连
func (c *Consumer) keepConnect() {
	for !c.exit {
		select {
		case <-c.reConnect:
			if c.status != KafkaConsumerDisconnected {
				break
			}

			logger.Warn("KafkaConsumer reconnecting", zap.Any(c.groupID, c.topics))
			var consumer *cluster.Consumer
		breakLoop:
			for {
				//利用熔断器给集群以恢复时间，避免不断的发送重联
				err := c.breaker.Run(func() (err error) {
					consumer, err = cluster.NewConsumer(c.hosts, c.groupID, c.topics, c.config)
					return
				})

				switch err {
				case nil:
					c.statusLock.Lock()
					if c.status == KafkaConsumerDisconnected {
						c.consumer = consumer
						c.status = KafkaConsumerConnected
					}
					c.statusLock.Unlock()
					break breakLoop
				case breaker.ErrBreakerOpen:
					logger.Warn("kafka  consumer connect fail, broker is open")
					//5 s之后，促发重新重连接，此时熔断器刚好 half close
					if c.status == KafkaConsumerDisconnected {
						time.AfterFunc(5*time.Second, func() {
							c.reConnect <- true
						})
					}
					break breakLoop
				default:
					logger.Error("kafka  consumer connect error", zap.Error(err))
				}
			}
		}
	}
}

//消费消息
func (c *Consumer) consumerMessage(f KafkaMessageHandler) {
	//在消费者未和 kafka 建立连接时，不消费 kafka 数据
	for !c.exit {
		if c.status != KafkaConsumerConnected {
			time.Sleep(time.Second * 5)
			logger.Warn("kafka consumer status " + c.status)
			continue
		}

		// Trap SIGINT to trigger a shutdown.
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

		// handle notifications
		go func() {
			for ntf := range c.consumer.Notifications() {
				logger.Warn("kafka consumer Rebalanced ", zap.Any(c.groupID, ntf))
			}
		}()

		// consume messages, watch signals
	ConsumerLoop:
		for !c.exit {
			select {
			case msg, ok := <-c.consumer.Messages():
				if ok {

					if commit, err := f(msg); commit {
						c.consumer.MarkOffset(msg, "") // mark message as processed
					} else {
						if err != nil {
							logger.Error("kafka consumer msg error ", zap.Error(err))
						}
					}
				}
			case err := <-c.consumer.Errors():
				logger.Error("kafka consumer msg error ", zap.Error(err))
				//需要捕获 kafka 中断信息，尝试重新连接 kafka
				if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
					c.statusLock.Lock()
					if c.status == KafkaConsumerConnected {
						c.status = KafkaConsumerDisconnected
						c.reConnect <- true
					}
					c.statusLock.Unlock()
				} else {
					// 如果不是中断信息,认为kafka挂了,进程退出
					// panic("kafka server error:" + err.Error())
				}
			case s := <-signals:
				// 收到系统消息先打印
				logger.Warn("kafka consumer receive system signal" + s.String())
				c.statusLock.Lock()
				c.exit = true
				//退出前先安全关闭
				err := c.consumer.Close()
				if err != nil {
					logger.Error("consumer.Close error", zap.Error(err))
				}
				c.statusLock.Unlock()
				break ConsumerLoop
			}
		}
	}
}
