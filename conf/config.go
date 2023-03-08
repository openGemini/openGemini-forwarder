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

package conf

import (
	"fmt"
	"io/ioutil"
	"path"

	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/toml"
	"github.com/influxdata/toml/ast"
	"github.com/openGemini/openGemini-forwarder/dag/node"
	"github.com/openGemini/openGemini-forwarder/plugins/inputs"
	"github.com/openGemini/openGemini-forwarder/plugins/outputs"
	"github.com/openGemini/openGemini-forwarder/plugins/parsers"
)

type Config struct {
	toml    *toml.Config
	Logging *Logger           `toml:"logging"`
	TLS     *tlsconfig.Config `toml:"tls"`
	Http    *Http             `toml:"http"`

	Inputs  []node.Node
	Outputs []node.Node
	Parsers []node.Node
}

func NewConfig() *Config {
	tls := tlsconfig.NewConfig()
	return &Config{
		toml: &toml.Config{
			NormFieldName: toml.DefaultConfig.NormFieldName,
			FieldToKey:    toml.DefaultConfig.FieldToKey},
		Http:    NewHttpConfig(),
		Logging: NewLogger("forwarder"),
		TLS:     &tls,
	}
}

func (c *Config) GetLogConfig() Logger {
	return *c.Logging
}

type Validator interface {
	Validate() error
}

func (c *Config) Validate() error {
	items := []Validator{
		c.Logging,
	}

	for _, item := range items {
		if err := item.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func Parse(conf *Config, path string) error {
	if path == "" {
		return nil
	}

	return fromTomlFile(conf, path)
}

func fromTomlFile(c *Config, p string) error {
	content, err := ioutil.ReadFile(path.Clean(p))
	if err != nil {
		return err
	}

	table, err := toml.Parse(content)
	if err != nil {
		return err
	}

	for name, value := range table.Fields {
		t, ok := value.(*ast.Table)
		if !ok {
			return fmt.Errorf("%v config format error", name)
		}
		switch name {
		case "logging":
			if err = c.toml.UnmarshalTable(t, c.Logging); err != nil {
				return err
			}
		case "tls":
			if err = c.toml.UnmarshalTable(t, c.TLS); err != nil {
				return err
			}
		case "http":
			if err = c.toml.UnmarshalTable(t, c.Http); err != nil {
				return err
			}
		case "inputs":
			inputs := inputs.GetInputs()
			err = ParsePlugins(t, INPUT, c, inputs)
			if err != nil {
				return fmt.Errorf("inputs plugins fail %v", err)
			}
		case "parsers":
			parsers := parsers.GetParsers()
			err = ParsePlugins(t, PARSER, c, parsers)
			if err != nil {
				return fmt.Errorf("parsers plugins fail %v", err)
			}
		case "outputs":
			outputs := outputs.GetOutputs()
			err = ParsePlugins(t, OUTPUT, c, outputs)
			if err != nil {
				return fmt.Errorf("output plugins fail %v", err)
			}
		}
	}
	return nil
}

func ParsePlugins(t *ast.Table, ty PluginType, c *Config, creator map[string]node.Creator) error {
	var ps *[]node.Node
	if ty == INPUT {
		ps = &c.Inputs
	} else if ty == OUTPUT {
		ps = &c.Outputs
	} else {
		ps = &c.Parsers
	}

	for name, v := range t.Fields {
		plugin, ok := creator[name]
		if !ok {
			return fmt.Errorf("undefined plugin %v", name)
		}
		p := plugin()
		vv, ok := v.([]*ast.Table)
		if !ok || len(vv) != 1 {
			return fmt.Errorf("%v config format error", name)
		}
		if err := c.toml.UnmarshalTable(vv[0], p); err != nil {
			return err
		}
		*ps = append(*ps, p)
	}
	return nil
}
