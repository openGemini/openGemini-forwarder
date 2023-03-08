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
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/openGemini/openGemini-forwarder/lib/logger"
	"go.uber.org/zap"
)

const ForwarderLOGO = `
 _________   ______         
|  _   _  |.' ____ \     
|_/ | | \_|| (___ \_|  
    | |     _.____.    
   _| |_   | \____) |
  |_____|   \______.'
	
`

const ForwarderUsage = `Runs the TSMeta server.

Usage: ts-forwarder run [flags]

    -config <path>
            Set the path to the configuration file.
            This defaults to the environment variable FORWARDER_CONFIG_PATH,
            ~/.ts/meta.conf, or /etc/ts/forwarder.conf if a file
            is present at any of these locations.
            Disable the automatic loading of a configuration file using
            the null device (such as /dev/null).
    -pidfile <path>
            Write process ID to a file.
    -cpuprofile <path>
            Write CPU profiling information to a file.
    -memprofile <path>
            Write memory usage information to a file.
`

// BuildInfo represents the build details for the server code.
type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
	Time    string
}

// Options represents the command line options that can be parsed.
type Options struct {
	ConfigPath string
	PIDFile    string
	Join       string
	Hostname   string
}

func (opt *Options) GetConfigPath() string {
	if opt.ConfigPath != "" {
		return opt.ConfigPath
	}

	return ""
}

func ParseFlags(usage func(), args ...string) (Options, error) {
	var options Options
	fs := flag.NewFlagSet("", flag.ExitOnError)
	fs.StringVar(&options.ConfigPath, "config", "", "")
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}
	return options, nil
}

func RemovePIDFile(pidfile string) {
	if pidfile == "" {
		return
	}

	if err := os.Remove(pidfile); err != nil {
		logger.GetLogger().Error("Remove pidfile failed", zap.Error(err))
	}
}

func WritePIDFile(pidfile string) error {
	if pidfile == "" {
		return nil
	}

	pidDir := filepath.Dir(pidfile)
	if err := os.MkdirAll(pidDir, 0700); err != nil {
		return fmt.Errorf("os.MkdirAll failed, error: %s", err)
	}

	pid := strconv.Itoa(os.Getpid())
	if err := ioutil.WriteFile(pidfile, []byte(pid), 0600); err != nil {
		return fmt.Errorf("write pid file failed, error: %s", err)
	}

	return nil
}
