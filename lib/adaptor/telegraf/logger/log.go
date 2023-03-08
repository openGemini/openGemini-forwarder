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

package logger

import (
	"fmt"

	"github.com/openGemini/openGemini-forwarder/lib/logger"
)

type Logger struct {
	Log *logger.Logger
}

func (l Logger) Errorf(format string, args ...interface{}) {
	l.Log.Error(fmt.Sprintf(format, args))
}

func (l Logger) Error(args ...interface{}) {
	l.Log.Error(fmt.Sprint(args))
}

func (l Logger) Debugf(format string, args ...interface{}) {
	l.Log.Debug(fmt.Sprintf(format, args))
}

func (l Logger) Debug(args ...interface{}) {
	l.Log.Debug(fmt.Sprint(args))
}

func (l Logger) Warnf(format string, args ...interface{}) {
	l.Log.Warn(fmt.Sprintf(format, args))
}

func (l Logger) Warn(args ...interface{}) {
	l.Log.Warn(fmt.Sprint(args))
}

func (l Logger) Infof(format string, args ...interface{}) {
	l.Log.Info(fmt.Sprintf(format, args))
}

func (l Logger) Info(args ...interface{}) {
	l.Log.Info(fmt.Sprint(args))
}
