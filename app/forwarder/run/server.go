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

package run

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/influxdata/influxdb/tcp"
	"github.com/openGemini/openGemini-forwarder/conf"
	"github.com/openGemini/openGemini-forwarder/dag"
	"github.com/openGemini/openGemini-forwarder/lib/logger"
	_ "github.com/openGemini/openGemini-forwarder/plugins"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type Server struct {
	cmd *cobra.Command

	BindAddress string
	Listener    net.Listener

	Logger *logger.Logger
	Conf   *conf.Config
}

// NewServer returns a new instance of Server built from a config.
func NewServer(conf *conf.Config, cmd *cobra.Command, logger *logger.Logger) (*Server, error) {
	bind := conf.Http.BindAddress

	s := &Server{
		cmd:         cmd,
		BindAddress: bind,
		Logger:      logger,
		Conf:        conf,
	}

	runtime.SetBlockProfileRate(int(1 * time.Second))
	runtime.SetMutexProfileFraction(1)
	return s, nil
}

func (s *Server) Err() <-chan error { return nil }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Mark start-up in log.
	s.Logger.Info("TS-Forwarder starting",
		zap.String("version", s.cmd.Version),
		zap.String("branch", s.cmd.ValidArgs[0]),
		zap.String("commit", s.cmd.ValidArgs[1]),
		zap.String("buildTime", s.cmd.ValidArgs[2]))
	s.Logger.Info("Go runtime",
		zap.String("version", runtime.Version()),
		zap.Int("maxprocs", cpu.GetCpuNum()))
	//Mark start-up in extra log
	fmt.Printf("%v TS-Forwarder  starting\n", time.Now())

	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	if s.Conf.Http.PprofEnabled {
		host, _, err := net.SplitHostPort(s.BindAddress)
		if err == nil {
			go func() {
				_ = http.ListenAndServe(net.JoinHostPort(host, "6062"), nil)
			}()
		}
	}

	// Multiplex listener.
	mux := tcp.NewMux(tcp.MuxLogger(os.Stdout))
	go func() {
		if err := mux.Serve(ln); err != nil {
			s.Logger.Error("listen failed",
				zap.String("addr", ln.Addr().String()),
				zap.Error(err))
		}
	}()

	dag, err := dag.NewDag(s.Conf)
	if err != nil {
		return err
	}
	err = dag.Init()
	if err != nil {
		return err
	}
	go func() {
		dag.Start()
	}()

	return nil
}

// Close shuts down the meta service.
func (s *Server) Close() error {
	var err error
	// Close the listener first to stop any new connections
	if s.Listener != nil {
		err = s.Listener.Close()
	}

	return err
}

// Service represents a service attached to the server.
type Service interface {
	Open() error
	Close() error
}
