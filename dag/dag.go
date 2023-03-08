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

package dag

import (
	"errors"

	"github.com/openGemini/openGemini-forwarder/conf"
	nodeModel "github.com/openGemini/openGemini-forwarder/dag/node"
	"github.com/openGemini/openGemini-forwarder/edge"
)

var (
	DefaultEdgeSize = 10
)

type Dag struct {
	root *node
}

func NewDag(c *conf.Config) (*Dag, error) {
	inputs := c.Inputs
	parsers := c.Parsers
	outputs := c.Outputs
	if len(inputs) != 1 || len(parsers) != 1 || len(outputs) != 1 {
		return nil, errors.New("inputs or parsers or outputs more than one")
	}

	var input *node
	for i := range inputs {
		input = &node{n: inputs[i], name: inputs[i].Name()}
	}
	var parser *node
	for i := range parsers {
		parser = &node{n: parsers[i], name: parsers[i].Name()}
	}
	var output *node
	for i := range outputs {
		output = &node{n: outputs[i], name: outputs[i].Name()}
	}

	input.LinkChild(parser, DefaultEdgeSize)
	parser.LinkChild(output, DefaultEdgeSize)
	dag := &Dag{root: input}
	return dag, nil
}

func (d Dag) Init() error {
	return d.root.Init()
}

func (d Dag) Start() {
	d.root.Start()
}

type node struct {
	parent *node
	child  *node
	in     edge.Edge
	out    edge.Edge
	n      nodeModel.Node
	name   string
}

func (n *node) Start() {
	n.n.Start(n.in, n.out)
	if n.child != nil {
		n.child.Start()
	}
}

func (n *node) Init() error {
	err := n.n.Init()
	if err != nil {
		return err
	}
	if n.child != nil {
		err = n.child.Init()
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *node) LinkChild(child *node, cacheSize int) {
	edge := edge.NewEdge(n.name, cacheSize)
	n.out = edge
	child.in = edge
	n.child = child
}
