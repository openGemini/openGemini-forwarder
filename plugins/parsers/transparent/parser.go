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

package transparent

import (
	"context"
	"sync"

	"github.com/openGemini/openGemini-forwarder/dag/node"
	"github.com/openGemini/openGemini-forwarder/edge"
	"github.com/openGemini/openGemini-forwarder/plugins/parsers"
)

type Parser struct {
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func (p *Parser) Name() string {
	return "transparent"
}

func (p *Parser) Init() error {
	return nil
}

func (p *Parser) Start(in edge.Edge, out edge.Edge) error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case record := <-in.Out():
				out.In() <- record
			}
		}
	}()
	return nil
}

func (p *Parser) Stop() error {
	p.cancel()
	p.wg.Wait()
	return nil
}

func init() {
	parsers.Add("transparent", func() node.Node {
		return &Parser{}
	})
}
