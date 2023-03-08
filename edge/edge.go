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

package edge

type Edge interface {
	Out() chan Record
	In() chan Record
}

func NewEdge(name string, size int) *StatEdge {
	edge := make(chan Record, size)
	return &StatEdge{name: name, edge: edge}
}

type StatEdge struct {
	name string
	edge chan Record
}

func (e *StatEdge) Out() chan Record {
	return e.edge
}

func (e *StatEdge) In() chan Record {
	return e.edge
}
