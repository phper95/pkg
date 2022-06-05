package mq

import (
	"fmt"
	"gitee.com/phper95/pkg/errors"
	"github.com/Shopify/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"
)

type ikafkaProducer interface {
	send() error
	close()
}

type KafkaProducer struct {
	Name       string `json:"name"`
	hosts      []string
	config     *sarama.Config
	status     string
	breaker    *breaker.Breaker
	reConnect  chan bool
	statusLock sync.Mutex
}

// kafka发送消息的结构体
type KafkaMsg struct {
	Topic     string
	KeyBytes  []byte
	DataBytes []byte
}

//同步生产者
type SyncProducer struct {
	KafkaProducer
	SyncProducer *sarama.SyncProducer
}

//异步生产者
type AsyncProducer struct {
	KafkaProducer
	asyncProducer *sarama.AsyncProducer
}

const (
	//生产者已连接
	KafkaProducerConnected string = "connected"
	//生产者已断开
	KafkaProducerDisconnected string = "disconnected"
	//生产者已关闭
	KafkaProducerClosed string = "closed"
)

var kafkaSyncProducers = make(map[string]*SyncProducer)
var kafkaAsyncProducers = make(map[string]*AsyncProducer)

//kafka默认生产者配置
func getDefaultProducerConfig(clientID string) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V2_0_0_0

	config.Net.DialTimeout = time.Second * 30
	config.Net.WriteTimeout = time.Second * 30
	config.Net.ReadTimeout = time.Second * 30

	config.Producer.Retry.Backoff = time.Millisecond * 500
	config.Producer.Retry.Max = 3

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	//需要小于broker的 `message.max.bytes`配置，默认是1000000
	config.Producer.MaxMessageBytes = 1000000 * 2

	config.Producer.RequiredAcks = sarama.WaitForLocal
	//config.Producer.Partitioner = sarama.NewRandomPartitioner
	//config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Partitioner = sarama.NewHashPartitioner
	//config.Producer.Partitioner = sarama.NewReferenceHashPartitioner

	// zstd 算法有着最高的压缩比，而在吞吐量上的表现只能说中规中矩，LZ4 > Snappy > zstd 和 GZIP
	//LZ4 具有最高的吞吐性能，压缩比zstd > LZ4 > GZIP > Snappy
	//综上LZ4性价比最高
	config.Producer.Compression = sarama.CompressionLZ4
	return
}

func InitSyncKakfaProducer(name string, hosts []string, config *sarama.Config) error {
	syncProducer := &SyncProducer{
		hosts:  hosts,
		config: makeProducerConfig(ClientID, lz4),
		status: ProducerDisconnected,

		breaker:   breaker.New(3, 1, 10*time.Second),
		reConnect: make(chan bool),
		//buffer: make(chan *sarama.ProducerMessage),
	}

	if config == nil {
		config = getDefaultProducerConfig(name)
	}

	if producer, err := sarama.NewSyncProducer(hosts, config); err != nil {
		return errors.Wrap()
		logs.Error("kafka connect error, ClientID: %s, error: %s", ClientID, err.Error())
		panic(fmt.Sprintf("kafka connect error, ClientID: %s, error: %s", ClientID, err.Error()))
	} else {
		syncProducer.syncProducer = &producer
		syncProducer.status = ProducerConnected
		logs.Debug("kafka connect suc, ClientID: %s", ClientID)
	}
	go syncProducer.keepConnect()
	go syncProducer.checkProducerWork()
}

func InitAsyncKakfaProducer(name string, host []string) {

	newAsyncProducerPool("default", clusterInfo)
}

func getAsyncProducerName(name string) string {
	return "async-" + name
}
func getSyncProducerName(name string) string {
	return "sync-" + name
}

// GetSyncKafkaProducer 获取对应 kafka 集群的同步生产者
func GetKafkaProducer() *SyncProducer {
	if clusterInfo == nil {
		panic("kafka  must be init before call this function")
	}
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(clusterInfo.DefaultProducers)
	return syncProducerPools[index]
}

func initSyncProducerPool(name string, ProducerNum int) {
	for i := 0; i < ProducerNum; i++ {
		syncProducerPools = append(syncProducerPools,
			NewSyncProducer(config.KafkaHosts, fmt.Sprintf("%s-%d", name, i), true))
	}
}

// GetAsyncKafkaProducer 获取对应 kafka 集群的同步生产者
func GetAsyncKafkaProducer() *AsyncProducer {
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(clusterInfo.DefaultProducers)
	if asyncProducerPools == nil {
		panic("kafka  must be init before call this function")
	}
	return asyncProducerPools[index]
}

func initAsyncProducerPool(name string, config *queue_config.KafkaClusterConfig) {
	for i := 0; i < config.DefaultProducers; i++ {
		asyncProducerPools = append(asyncProducerPools,
			NewAsyncProducer(config.KafkaHosts, fmt.Sprintf("%s-%d", name, i), true))
	}
}

//检查kafka连接状态,如果断开链接则尝试重连
func (syncProducer *SyncProducer) keepConnect() {
	for {
		select {
		case <-syncProducer.reConnect:
			if syncProducer.status != ProducerDisconnected {
				break
			}

			logs.Warn("kafka reconnecting. ClientID: %s", syncProducer.config.ClientID)
			var producer sarama.SyncProducer
		breakLoop:
			for {
				//利用熔断器给集群以恢复时间，避免不断的发送重联
				err := syncProducer.breaker.Run(func() (err error) {
					producer, err = sarama.NewSyncProducer(syncProducer.hosts, syncProducer.config)
					return
				})

				switch err {
				case nil:
					syncProducer.statusLock.Lock()
					if syncProducer.status == ProducerDisconnected {
						syncProducer.syncProducer = &producer
						syncProducer.status = ProducerConnected
					}
					syncProducer.statusLock.Unlock()
					logs.Info("kafka reconnect suc, ClientID: %s", syncProducer.config.ClientID)
					break breakLoop
				case breaker.ErrBreakerOpen:
					logs.Debug("kafka connect fail, broker is open")
					//10 分钟之后，促发重新重联，此时熔断器刚好 half close
					if syncProducer.status == ProducerDisconnected {
						logs.Debug("begin time clock %s", time.Now())
						time.AfterFunc(10*time.Second, func() {
							logs.Debug("active reconnect select, time is %s", time.Now())
							syncProducer.reConnect <- true
						})
						logs.Debug("end time clock %s", time.Now())
					}
					break breakLoop
				default:
					logs.Error("kafka reconnect failed, ClientID: %s, error: %s", syncProducer.config.ClientID, err.Error())
				}
			}
		}
	}
}

//检查异步生产者的发送反馈
func (syncProducer *SyncProducer) checkProducerWork() {
	defer func() {
		if syncProducer.syncProducer != nil {
			syncProducer.statusLock.Lock()
			syncProducer.status = ProducerClosed
			syncProducer.statusLock.Unlock()
			if err := (*syncProducer.syncProducer).Close(); err != nil {
				logs.Error(err)
			}
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

ProducerLoop:
	for {
		select {
		case <-signals:
			break ProducerLoop
		}
	}
}

//SendMsgs 同步发送消息到 kafka
func (syncProducer *SyncProducer) SendMsgs(sendMsgs ...*MsgToSend) ([]*MsgToSend, error) {
	if syncProducer.status != ProducerConnected {
		return sendMsgs, errors.New("kafka disconnected")
	}
	msgs := make([]*sarama.ProducerMessage, len(sendMsgs))
	for i, outputMsg := range sendMsgs {
		msg := &sarama.ProducerMessage{
			Topic: outputMsg.OutputTopic,
			Value: sarama.ByteEncoder(outputMsg.DataBytes),
		}
		if outputMsg.KeyBytes != nil && len(outputMsg.KeyBytes) != 0 {
			msg.Key = sarama.ByteEncoder(outputMsg.KeyBytes)
		}
		msgs[i] = msg
	}
	errs := (*syncProducer.syncProducer).SendMessages(msgs)
	if errs == nil {
		return nil, nil
	}
	var failedOutputMsgs []*MsgToSend
	for _, err := range errs.(sarama.ProducerErrors) {
		logs.Error("Failed to produce message, err type :", reflect.TypeOf(err))
		logs.Error("Failed to produce message", err)

		if err.Error() == "EOF" {
			logs.Error("kafka cluster connect EOF")
			syncProducer.statusLock.Lock()
			if syncProducer.status == ProducerConnected {
				syncProducer.status = ProducerDisconnected
				syncProducer.reConnect <- true
			}
			syncProducer.statusLock.Unlock()
		}
		failedMsg := &MsgToSend{
			OutputTopic: err.Msg.Topic,
		}
		failedMsg.DataBytes, _ = err.Msg.Value.Encode()
		if err.Msg.Key != nil {
			failedMsg.KeyBytes, _ = err.Msg.Key.Encode()
		}
		failedOutputMsgs = append(failedOutputMsgs, failedMsg)
	}
	return failedOutputMsgs, errs
}

//SendMsg 同步发送消息到 kafka
func (syncProducer *SyncProducer) SendMsg(sendMsg *MsgToSend) (*MsgToSend, error) {
	if syncProducer.status != ProducerConnected {
		return sendMsg, errors.New("kafka disconnected")
	}

	msg := &sarama.ProducerMessage{
		Topic: sendMsg.OutputTopic,
		Value: sarama.ByteEncoder(sendMsg.DataBytes),
	}
	if sendMsg.KeyBytes != nil && len(sendMsg.KeyBytes) != 0 {
		msg.Key = sarama.ByteEncoder(sendMsg.KeyBytes)
	}

	_, _, err := (*syncProducer.syncProducer).SendMessage(msg)
	if err == nil {
		return nil, nil
	}

	//重试一次
	logs.Debug("Failed to produce message first, try second time:")
	_, _, err = (*syncProducer.syncProducer).SendMessage(msg)
	if err == nil {
		return nil, nil
	}

	logs.Error("Fail to produce message topic: %s key: %s message: %s\n", msg.Topic, msg.Key, msg.Value)
	logs.Debug("Failed to produce message, err type :", reflect.TypeOf(err))
	logs.Error("Failed to produce message", err)

	if err.Error() == "EOF" {
		logs.Debug("kafka cluster connect EOF")
		syncProducer.statusLock.Lock()
		if syncProducer.status == ProducerConnected {
			syncProducer.status = ProducerDisconnected
			syncProducer.reConnect <- true
		}
		syncProducer.statusLock.Unlock()
	}

	return sendMsg, err
}

//初始化同步生产者
func initAsyncProducer(name string, hosts []string, ClientID string, lz4 bool) (asyncProducer *AsyncProducer) {
	asyncProducer = &AsyncProducer{
		hosts:  hosts,
		config: makeProducerConfig(ClientID, lz4),
		status: ProducerDisconnected,

		breaker:   breaker.New(3, 1, 10*time.Second),
		reConnect: make(chan bool),
		//buffer: make(chan *sarama.ProducerMessage),
	}

	if producer, err := sarama.NewAsyncProducer(hosts, asyncProducer.config); err != nil {
		logs.Error("kafka connect error, ClientID: %s, error: %s", ClientID, err.Error())
		panic(fmt.Sprintf("kafka connect error, ClientID: %s, error: %s", ClientID, err.Error()))
	} else {
		asyncProducer.asyncProducer = &producer
		asyncProducer.status = ProducerConnected
		logs.Debug("kafka connect suc, ClientID: %s", ClientID)
	}
	go asyncProducer.keepConnect()
	go asyncProducer.checkProducerWork()
	//go asyncProducer.bufferHandler()
	return
}

//检查kafka连接状态,如果断开链接则尝试重连
func (asyncProducer *AsyncProducer) keepConnect() {
	for {
		select {
		case <-asyncProducer.reConnect:
			if asyncProducer.status != ProducerDisconnected {
				break
			}

			logs.Warn("kafka reconnecting. ClientID: %s", asyncProducer.config.ClientID)
			var producer sarama.AsyncProducer
		breakLoop:
			for {
				//利用熔断器给集群以恢复时间，避免不断的发送重联
				err := asyncProducer.breaker.Run(func() (err error) {
					producer, err = sarama.NewAsyncProducer(asyncProducer.hosts, asyncProducer.config)
					return
				})

				switch err {
				case nil:
					asyncProducer.statusLock.Lock()
					if asyncProducer.status == ProducerDisconnected {
						asyncProducer.asyncProducer = &producer
						asyncProducer.status = ProducerConnected
					}
					asyncProducer.statusLock.Unlock()
					logs.Info("kafka reconnect suc, ClientID: %s", asyncProducer.config.ClientID)
					break breakLoop
				case breaker.ErrBreakerOpen:
					logs.Debug("begin time clock %s", time.Now())
					//10 分钟之后，促发重新重联，此时熔断器刚好 half close
					if asyncProducer.status == ProducerDisconnected {
						time.AfterFunc(10*time.Second, func() {
							logs.Debug("active reconnect select, time is %s", time.Now())
							asyncProducer.reConnect <- true
						})
						logs.Debug("end time clock %s", time.Now())
					}
					break breakLoop
				default:
					logs.Error("kafka reconnect failed, ClientID: %s, error: %s", asyncProducer.config.ClientID, err.Error())
				}
			}
		}
	}

	/*	for {
		time.Sleep(time.Second * 10)
		if asyncProducer.status == ProducerConnected {
			continue
		}
		logs.Warn(fmt.Sprintf("kafka reconnecting. ClientID: %s", asyncProducer.config.ClientID))
		if producer, err := sarama.NewAsyncProducer(asyncProducer.hosts, asyncProducer.config); err != nil {
			logs.Error(fmt.Sprintf("kafka reconnect failed, ClientID: %s, error: %s", asyncProducer.config.ClientID, err.Error()))
		} else {
			asyncProducer.asyncProducer = &producer
			asyncProducer.status = ProducerConnected
			logs.Info(fmt.Sprintf("kafka reconnect suc, ClientID: %s", asyncProducer.config.ClientID))
		}
	}*/
}

//检查异步生产者的发送反馈
func (asyncProducer *AsyncProducer) checkProducerWork() {
	defer func() {
		if asyncProducer.asyncProducer != nil {
			asyncProducer.statusLock.Lock()
			asyncProducer.status = ProducerClosed
			asyncProducer.statusLock.Unlock()
			if err := (*asyncProducer.asyncProducer).Close(); err != nil {
				logs.Error(err)
			}
		}
	}()

	//在生产者未和 kafka 建立连接时，不进行生产者的相关检查
	exit := false
	for !exit {
		if asyncProducer.status != ProducerConnected {
			time.Sleep(time.Second * 10)
			continue
		}
		// Trap SIGINT to trigger a shutdown.
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	ProducerLoop:
		for {
			select {
			case msg := <-(*asyncProducer.asyncProducer).Successes():
				logs.Info("Success to produce message : topic %s , value %s ", msg.Topic, msg.Value)
			case err := <-(*asyncProducer.asyncProducer).Errors():
				msg := err.Msg
				logs.Error("message send fail topic: %s partition: %d offset: %d key: %s message: %s\n",
					msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				if err.Error() == "EOF" {
					logs.Error("kafka cluster connect EOF")
					// producer 连接中断，是否需要释放 producer ？ 此处需要进行测试
					// 一般而言，捕捉不到 EOF
					asyncProducer.statusLock.Lock()
					if asyncProducer.status == ProducerConnected {
						asyncProducer.status = ProducerDisconnected
						asyncProducer.reConnect <- true
					}
					asyncProducer.statusLock.Unlock()
				}
				logs.Error("Failed to produce message, err type :", reflect.TypeOf(err))
				logs.Error("Failed to produce message", err)
			case s := <-signals:
				logs.Warn("[kafka] producer receive system signal", s.String())
				asyncProducer.statusLock.Lock()
				exit = true
				asyncProducer.statusLock.Unlock()
				break ProducerLoop
			}
		}
	}
}

//SendMsg 同步发送消息到 kafka
func (asyncProducer *AsyncProducer) SendMsg(msg *MsgToSend) error {
	if asyncProducer.status != ProducerConnected {
		return errors.New("kafka disconnected")
	}

	message := &sarama.ProducerMessage{
		Topic: msg.OutputTopic,
		Value: sarama.ByteEncoder(msg.DataBytes),
	}
	if msg.KeyBytes != nil && len(msg.KeyBytes) != 0 {
		message.Key = sarama.ByteEncoder(msg.KeyBytes)
	}

	(*asyncProducer.asyncProducer).Input() <- message
	return nil
}
