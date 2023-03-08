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
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/openGemini/openGemini-forwarder/edge"
	"github.com/openGemini/openGemini-forwarder/lib/logger"
	"github.com/openGemini/openGemini-forwarder/lib/pool"
	"go.uber.org/zap"
)

func NewConsumerGroupHandler(edge edge.Edge, log logger.Logger) *ConsumerGroupHandler {
	handler := &ConsumerGroupHandler{
		log:  log,
		edge: edge,
	}
	return handler
}

// ConsumerGroupHandler is a sarama.ConsumerGroupHandler implementation.
type ConsumerGroupHandler struct {
	MaxMessageLen int
	TopicTag      string

	edge edge.Edge
	wg   sync.WaitGroup
	mu   sync.Mutex

	log             logger.Logger
	kafkaRecordPool *pool.KafkaRecordPool
}

// Setup is called once when a new session is opened.  It setups up the handler
// and begins processing delivered messages.
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	h.kafkaRecordPool = pool.NewKafkaRecordPool()
	return nil
}

// Handle processes a message and if successful saves it to be acknowledged
// after delivery.
func (h *ConsumerGroupHandler) Handle(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) error {
	if h.MaxMessageLen != 0 && len(msg.Value) > h.MaxMessageLen {
		session.MarkMessage(msg, "")
		return fmt.Errorf("message exceeds max_message_len (actual %d, max %d)",
			len(msg.Value), h.MaxMessageLen)
	}

	kafkaRecordPool := h.kafkaRecordPool.Get()
	kafkaRecordPool.Message = msg
	kafkaRecordPool.Session = session
	h.edge.In() <- kafkaRecordPool
	return nil
}

// ConsumeClaim is called once each claim in a goroutine and must be
// thread-safe.  Should run until the claim is closed.
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			err := h.Handle(session, msg)
			if err != nil {
				h.log.Error("handle msg fail", zap.Error(err))
			}
		}
	}
}

// Cleanup stops the internal goroutine and is called after all ConsumeClaim
// functions have completed.
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.wg.Wait()
	return nil
}
