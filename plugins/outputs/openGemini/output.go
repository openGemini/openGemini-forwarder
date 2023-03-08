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

package openGemini

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/openGemini/openGemini-forwarder/dag/node"
	"github.com/openGemini/openGemini-forwarder/edge"
	"github.com/openGemini/openGemini-forwarder/lib/pool"
	"github.com/openGemini/openGemini-forwarder/plugins/outputs"
)

var (
	defaultURL = "http://localhost:8086"
)

type Output struct {
	OpenGemini

	clients   []influxdb2.Client
	writeApis []api.WriteAPI

	wg     sync.WaitGroup
	cancel context.CancelFunc

	kafkaRecordPool *pool.KafkaRecordPool
}

func (o *Output) Name() string {
	return "openGemini"
}

func (o *Output) Stop() error {
	o.cancel()
	o.wg.Wait()
	for _, client := range o.clients {
		client.Close()
	}
	return nil
}

func (o *Output) Init() error {
	o.kafkaRecordPool = pool.NewKafkaRecordPool()
	urls := make([]string, 0, len(o.URLs))
	urls = append(urls, o.URLs...)
	if o.URL != "" {
		urls = append(urls, o.URL)
	}

	if len(urls) == 0 {
		urls = append(urls, defaultURL)
	}

	for _, u := range urls {
		parts, err := url.Parse(u)
		if err != nil {
			return fmt.Errorf("error parsing url [%q]: %v", u, err)
		}

		switch parts.Scheme {
		case "http", "https":
			c := influxdb2.NewClient(u, fmt.Sprintf("%v:%v", o.Username, o.Password))
			o.clients = append(o.clients, c)
			api := c.WriteAPI("", fmt.Sprintf("%v/%v", o.Database, o.RetentionPolicy))
			o.writeApis = append(o.writeApis, api)
		default:
			return fmt.Errorf("unsupported scheme [%q]: %q", u, parts.Scheme)
		}
	}

	return nil
}

func (o *Output) Start(in edge.Edge, _ edge.Edge) error {
	ctx, cancel := context.WithCancel(context.Background())
	o.cancel = cancel
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case record := <-in.Out():
				rec, ok := record.(*edge.KafkaRecord)
				if ok {
					o.writeApis[0].WriteRecord(string(rec.Message.Value))
					o.kafkaRecordPool.Put(rec)
				}
			}
		}
	}()
	return nil
}

func init() {
	outputs.Add("openGemini", func() node.Node {
		return &Output{}
	})
}
