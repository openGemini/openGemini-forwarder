package outputs

import (
	"github.com/openGemini/openGemini-forwarder/dag/node"
)

var outputs = map[string]node.Creator{}

func GetOutputs() map[string]node.Creator {
	return outputs
}

func Add(name string, creator node.Creator) {
	outputs[name] = creator
}
