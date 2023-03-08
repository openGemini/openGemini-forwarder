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

package pool

import (
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini-forwarder/edge"
)

type KafkaRecordPool struct {
	pool *sync.Pool

	hit   int64
	total int64
}

var kafkaRecordPool *KafkaRecordPool

func init() {
	kafkaRecordPool = &KafkaRecordPool{
		pool: new(sync.Pool),
	}
}

func NewKafkaRecordPool() *KafkaRecordPool {
	return kafkaRecordPool
}

func (u *KafkaRecordPool) Get() *edge.KafkaRecord {
	atomic.AddInt64(&u.total, 1)

	v, ok := u.pool.Get().(*edge.KafkaRecord)
	if !ok || v == nil {
		return &edge.KafkaRecord{}
	}

	atomic.AddInt64(&u.hit, 1)
	return v
}

func (u *KafkaRecordPool) Put(v *edge.KafkaRecord) {
	if v.Session != nil {
		v.Session.MarkMessage(v.Message, "")
		v.Session = nil
	}
	v.Message = nil
	u.pool.Put(v)
}

func (u *KafkaRecordPool) HitRatio() float64 {
	return float64(u.hit) / float64(u.total)
}
