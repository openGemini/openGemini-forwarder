/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/influxdata/telegraf/plugins/common/kafka"
	"github.com/openGemini/openGemini-forwarder/dag/node"
	"github.com/openGemini/openGemini-forwarder/edge"
	kafkalogger "github.com/openGemini/openGemini-forwarder/lib/adaptor/telegraf/logger"
	"github.com/openGemini/openGemini-forwarder/lib/logger"
	"github.com/openGemini/openGemini-forwarder/plugins/inputs"
)

var sampleConfig string

const (
	defaultMaxUndeliveredMessages = 1000
	defaultMaxProcessingTime      = time.Duration(100 * time.Millisecond)
	defaultConsumerGroup          = "telegraf_metrics_consumers"
	reconnectDelay                = 5 * time.Second
)

type Input struct {
	Brokers                []string      `toml:"brokers"`
	ConsumerGroup          string        `toml:"consumer_group"`
	MaxMessageLen          int           `toml:"max_message_len"`
	MaxUndeliveredMessages int           `toml:"max_undelivered_messages"`
	MaxProcessingTime      time.Duration `toml:"max_processing_time"`
	Offset                 string        `toml:"offset"`
	BalanceStrategy        string        `toml:"balance_strategy"`
	Topics                 []string      `toml:"topics"`
	TopicTag               string        `toml:"topic_tag"`
	ConsumerFetchDefault   int64         `toml:"consumer_fetch_default"`
	ConnectionStrategy     string        `toml:"connection_strategy"`

	kafka.ReadConfig

	kafka.Logger
	Log logger.Logger `toml:"-"`

	ConsumerCreator ConsumerGroupCreator `toml:"-"`
	consumer        ConsumerGroup
	config          *sarama.Config

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func (k *Input) Name() string {
	return "kafka_consumer"
}

type ConsumerGroup interface {
	Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
	Errors() <-chan error
	Close() error
}

type ConsumerGroupCreator interface {
	Create(brokers []string, group string, cfg *sarama.Config) (ConsumerGroup, error)
}

type SaramaCreator struct{}

func (*SaramaCreator) Create(brokers []string, group string, cfg *sarama.Config) (ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers, group, cfg)
}

func (k *Input) Init() error {
	k.SetLogger()

	if k.MaxUndeliveredMessages == 0 {
		k.MaxUndeliveredMessages = defaultMaxUndeliveredMessages
	}
	if time.Duration(k.MaxProcessingTime) == 0 {
		k.MaxProcessingTime = defaultMaxProcessingTime
	}
	if k.ConsumerGroup == "" {
		k.ConsumerGroup = defaultConsumerGroup
	}

	cfg := sarama.NewConfig()

	// Kafka version 0.10.2.0 is required for consumer groups.
	cfg.Version = sarama.V0_10_2_0

	if err := k.SetConfig(cfg, kafkalogger.Logger{Log: &k.Log}); err != nil {
		return fmt.Errorf("SetConfig: %w", err)
	}

	switch strings.ToLower(k.Offset) {
	case "oldest", "":
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		return fmt.Errorf("invalid offset %q", k.Offset)
	}

	switch strings.ToLower(k.BalanceStrategy) {
	case "range", "":
		cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	case "roundrobin":
		cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	case "sticky":
		cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	default:
		return fmt.Errorf("invalid balance strategy %q", k.BalanceStrategy)
	}

	if k.ConsumerCreator == nil {
		k.ConsumerCreator = &SaramaCreator{}
	}

	cfg.Consumer.MaxProcessingTime = time.Duration(k.MaxProcessingTime)

	if k.ConsumerFetchDefault != 0 {
		cfg.Consumer.Fetch.Default = int32(k.ConsumerFetchDefault)
	}

	switch strings.ToLower(k.ConnectionStrategy) {
	default:
		return fmt.Errorf("invalid connection strategy %q", k.ConnectionStrategy)
	case "defer", "startup", "":
	}

	k.config = cfg
	return nil
}

func (k *Input) create() error {
	var err error
	k.consumer, err = k.ConsumerCreator.Create(
		k.Brokers,
		k.ConsumerGroup,
		k.config,
	)

	return err
}

func (k *Input) startErrorAdder() {
	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		for err := range k.consumer.Errors() {
			k.Log.Error(fmt.Sprintf("channel: %v", err))
		}
	}()
}

func (k *Input) Start(_ edge.Edge, out edge.Edge) error {
	var err error

	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel

	if k.ConnectionStrategy != "defer" {
		err = k.create()
		if err != nil {
			return fmt.Errorf("create consumer: %w", err)
		}
		k.startErrorAdder()
	}

	// Start consumer goroutine
	k.wg.Add(1)
	go func() {
		var err error
		defer k.wg.Done()

		if k.consumer == nil {
			err = k.create()
			if err != nil {
				k.Log.Error(fmt.Sprintf("create consumer async: %w", err))
				return
			}
		}

		k.startErrorAdder()

		for ctx.Err() == nil {
			handler := NewConsumerGroupHandler(out, k.Log)
			handler.MaxMessageLen = k.MaxMessageLen
			handler.TopicTag = k.TopicTag
			err := k.consumer.Consume(ctx, k.Topics, handler)
			if err != nil {
				k.Log.Error(fmt.Sprintf("consume: %v", err))
				select {
				case <-time.After(reconnectDelay):
				case <-ctx.Done():
				}
			}
		}
		err = k.consumer.Close()
		if err != nil {
			k.Log.Error(fmt.Sprintf("close: %w", err))
		}
	}()

	return nil
}

func (k *Input) Stop() error {
	k.cancel()
	k.wg.Wait()
	return nil
}

func init() {
	inputs.Add("kafka_consumer", func() node.Node {
		return &Input{}
	})
}
