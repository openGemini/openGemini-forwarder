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

package app

import (
	"fmt"

	"github.com/openGemini/openGemini-forwarder/conf"
	"github.com/openGemini/openGemini-forwarder/lib/logger"
	"github.com/spf13/cobra"
)

// Command represents the command executed by "ts-xxx run".
type Command struct {
	Logo          string
	Usage         string
	closing       chan struct{}
	Pidfile       string
	Closed        chan struct{}
	Logger        *logger.Logger
	Command       *cobra.Command
	ServiceName   string
	Server        Server
	Config        *conf.Config
	NewServerFunc func(conf.Config, *cobra.Command, *logger.Logger) (Server, error)
}

func (cmd *Command) Close() error {
	return nil
}

type Server interface {
	Open() error
	Close() error
	Err() <-chan error
}

func NewCommand() *Command {
	return &Command{
		closing: make(chan struct{}),
		Closed:  make(chan struct{}),
	}
}

func (cmd *Command) InitConfig(c *conf.Config, path string) error {
	if err := conf.Parse(c, path); err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	if err := c.Validate(); err != nil {
		return err
	}

	cmd.Config = c
	return nil
}
