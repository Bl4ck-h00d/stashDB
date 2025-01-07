package marshaler

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/Bl4ck-h00d/stashdb/types"
	"github.com/golang/protobuf/ptypes/any"
)

func init() {
	types.RegisterType("protobuf.LivenessCheckResponse", reflect.TypeOf(protobuf.HeartbeatCheckResponse{}))
	types.RegisterType("protobuf.Metadata", reflect.TypeOf(protobuf.Metadata{}))
	types.RegisterType("protobuf.Node", reflect.TypeOf(protobuf.Node{}))
	types.RegisterType("protobuf.Cluster", reflect.TypeOf(protobuf.Cluster{}))
	types.RegisterType("protobuf.JoinRequest", reflect.TypeOf(protobuf.JoinRequest{}))
	types.RegisterType("protobuf.LeaveRequest", reflect.TypeOf(protobuf.LeaveRequest{}))
	types.RegisterType("protobuf.NodeResponse", reflect.TypeOf(protobuf.NodeResponse{}))
	types.RegisterType("protobuf.ClusterResponse", reflect.TypeOf(protobuf.ClusterResponse{}))
	types.RegisterType("protobuf.GetRequest", reflect.TypeOf(protobuf.GetRequest{}))
	types.RegisterType("protobuf.GetResponse", reflect.TypeOf(protobuf.GetResponse{}))
	types.RegisterType("protobuf.SetRequest", reflect.TypeOf(protobuf.SetRequest{}))
	types.RegisterType("protobuf.DeleteRequest", reflect.TypeOf(protobuf.DeleteRequest{}))
	types.RegisterType("protobuf.SetMetadataRequest", reflect.TypeOf(protobuf.SetMetadataRequest{}))
	types.RegisterType("protobuf.DeleteMetadataRequest", reflect.TypeOf(protobuf.DeleteMetadataRequest{}))
	types.RegisterType("protobuf.Event", reflect.TypeOf(protobuf.Event{}))
	types.RegisterType("protobuf.WatchResponse", reflect.TypeOf(protobuf.WatchResponse{}))
	types.RegisterType("protobuf.KeyValuePair", reflect.TypeOf(protobuf.KeyValuePair{}))
	types.RegisterType("map[string]interface {}", reflect.TypeOf((map[string]interface{})(nil)))
}

func MarshalAny(message *any.Any) (interface{}, error) {
	if message == nil {
		return nil, nil
	}

	typeUrl := message.TypeUrl
	value := message.Value
	if len(value) == 0 {
		return nil, fmt.Errorf("message value is empty")
	}

	instance := types.TypeInstanceByName(typeUrl)

	if err := json.Unmarshal(value, instance); err != nil {
		return nil, err
	} else {
		return instance, nil
	}

}

func UnmarshalAny(instance interface{}, message *any.Any) error {
	if instance == nil {
		return nil
	}

	value, err := json.Marshal(instance)
	if err != nil {
		return err
	}

	message.TypeUrl = types.TypeNameByInstance(instance)
	message.Value = value

	return nil
}
