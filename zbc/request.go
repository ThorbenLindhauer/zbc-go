package zbc

import (
	"github.com/zeebe-io/zbc-go/zbc/protocol"
	"github.com/zeebe-io/zbc-go/zbc/sbe"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Task struct {
	State       string                 `yaml:"state" msgpack:"state"`
	Headers     map[string]interface{} `yaml:"headers" msgpack:"headers"`
	Retries     int                    `yaml:"retries" msgpack:"retries"`
	Type        string                 `yaml:"type" msgpack:"type"`
	Payload     []uint8                `yaml:"-" msgpack:"payload"`
	PayloadJson map[string]interface{} `yaml:"payload" msgpack:"-"`
}

type WorkflowInstance struct {
	State         string                 `yaml:"state" msgpack:"state"`
	BpmnProcessId string                 `yaml:"bpmnProcessId" msgpack:"bpmnProcessId"`
	Version       int                    `yaml:"version" msgpack:"version"`
	Payload       []uint8                `yaml:"-" msgpack:"payload"`
	PayloadJson   map[string]interface{} `yaml:"payload" msgpack:"-"`
}

type Deployment struct {
	State   string `yaml:"state" msgpack:"state"`
	BpmnXml []byte `yaml:"bpmnXml" msgpack:"bpmnXml"`
}

func NewCompleteTaskMessage(taskMessage *Message) *Message {
	payload := *taskMessage.Data
	payload["state"] = "COMPLETE"
	cmdReq := &sbe.ExecuteCommandRequest{
		PartitionId: (*taskMessage.SbeMessage).(*sbe.SubscribedEvent).PartitionId,
		Position:    (*taskMessage.SbeMessage).(*sbe.SubscribedEvent).Position,
		Key:         (*taskMessage.SbeMessage).(*sbe.SubscribedEvent).Key,
		TopicName:   (*taskMessage.SbeMessage).(*sbe.SubscribedEvent).TopicName,
	}

	return NewCommandRequestMessage(cmdReq, payload)
}

func NewTaskMessage(commandRequest *sbe.ExecuteCommandRequest, task *Task) *Message {
	commandRequest.EventType = sbe.EventTypeEnum(0)

	if task.Payload == nil {
		b, err := msgpack.Marshal(task.PayloadJson)
		if err != nil {
			return nil
		}
		task.Payload = b
	}

	return NewCommandRequestMessage(commandRequest, task)
}

func NewWorkflowMessage(commandRequest *sbe.ExecuteCommandRequest, wf *WorkflowInstance) *Message {
	commandRequest.EventType = sbe.EventTypeEnum(5)

	if wf.Payload == nil {
		b, err := msgpack.Marshal(wf.PayloadJson)
		if err != nil {
			return nil
		}
		wf.Payload = b
	}

	return NewCommandRequestMessage(commandRequest, wf)
}

func NewDeploymentMessage(commandRequest *sbe.ExecuteCommandRequest, d *Deployment) *Message {
	commandRequest.EventType = sbe.EventTypeEnum(4)
	return NewCommandRequestMessage(commandRequest, d)
}

func NewCommandRequestMessage(commandRequest *sbe.ExecuteCommandRequest, command interface{}) *Message {
	var msg Message

	b, err := msgpack.Marshal(command)
	if err != nil {
		return nil
	}
	commandRequest.Command = b
	msg.SetSbeMessage(commandRequest)

	// We add +2 to every variable length attribute since all variable length attributes will have 2 bytes in front
	// which will denote their size. Then we add 19 bytes which is size of non-variable length attributes of
	// ExecuteCommandRequest and 26 bytes which is for SbeMessageHeader, RequestResponse and Transport.
	length := uint32(LengthFieldSize+len(commandRequest.TopicName)) + uint32(LengthFieldSize+len(commandRequest.Command))
	length += uint32(commandRequest.SbeBlockLength()) +TotalHeaderSizeNoFrame

	var headers Headers
	headers.SetSbeMessageHeader(&sbe.MessageHeader{
		BlockLength: commandRequest.SbeBlockLength(),
		TemplateId:  commandRequest.SbeTemplateId(),
		SchemaId:    commandRequest.SbeSchemaId(),
		Version:     commandRequest.SbeSchemaVersion(),
	})

	headers.SetRequestResponseHeader(protocol.NewRequestResponseHeader())
	headers.SetTransportHeader(protocol.NewTransportHeader(protocol.RequestResponse))

	// Writer will set FrameHeader after serialization to byte array.
	headers.SetFrameHeader(protocol.NewFrameHeader(uint32(length), 0, 0, 0, 0))

	msg.SetHeaders(&headers)
	return &msg
}

// TaskSubscription is structure which we use to handle a subscription on a task.
type TaskSubscription struct {
	SubscriberKey uint64 `msgpack:"subscriberKey"`
	TopicName     string `msgpack:"topicName"`
	PartitionID   int32  `msgpack:"partitionId"`
	TaskType      string `msgpack:"taskType"`
	LockDuration  uint64 `msgpack:"lockDuration"`
	LockOwner     string `msgpack:"lockOwner"`
	Credits       int32  `msgpack:"credits"`
}

// NewTaskSubscriptionMessage is a constructor for Message object which will contain TaskSubscription as payload.
func NewTaskSubscriptionMessage(ts *TaskSubscription) *Message {
	var msg Message

	b, err := msgpack.Marshal(ts)
	if err != nil {
		return nil
	}
	controlRequest := &sbe.ControlMessageRequest{
		MessageType: sbe.ControlMessageType.ADD_TASK_SUBSCRIPTION,
		Data:        b,
	}
	msg.SetSbeMessage(controlRequest)

	length := 1 + uint32(LengthFieldSize+len(controlRequest.Data)) + TotalHeaderSizeNoFrame

	var headers Headers
	headers.SetSbeMessageHeader(&sbe.MessageHeader{
		BlockLength: controlRequest.SbeBlockLength(),
		TemplateId:  controlRequest.SbeTemplateId(),
		SchemaId:    controlRequest.SbeSchemaId(),
		Version:     controlRequest.SbeSchemaVersion(),
	})

	headers.SetRequestResponseHeader(protocol.NewRequestResponseHeader())
	headers.SetTransportHeader(protocol.NewTransportHeader(protocol.RequestResponse))
	headers.SetFrameHeader(protocol.NewFrameHeader(uint32(length), 0, 0, 0, 0))

	msg.SetHeaders(&headers)
	return &msg
}

func NewCloseTaskSubscriptionMessage(ts *TaskSubscription) *Message {
	var msg Message

	b, err := msgpack.Marshal(ts)
	if err != nil {
		return nil
	}
	controlRequest := &sbe.ControlMessageRequest{
		MessageType: sbe.ControlMessageType.REMOVE_TASK_SUBSCRIPTION,
		Data:        b,
	}
	msg.SetSbeMessage(controlRequest)

	length := 1 + uint32(LengthFieldSize+len(controlRequest.Data)) + TotalHeaderSizeNoFrame

	var headers Headers
	headers.SetSbeMessageHeader(&sbe.MessageHeader{
		BlockLength: controlRequest.SbeBlockLength(),
		TemplateId:  controlRequest.SbeTemplateId(),
		SchemaId:    controlRequest.SbeSchemaId(),
		Version:     controlRequest.SbeSchemaVersion(),
	})

	headers.SetRequestResponseHeader(protocol.NewRequestResponseHeader())
	headers.SetTransportHeader(protocol.NewTransportHeader(protocol.RequestResponse))
	headers.SetFrameHeader(protocol.NewFrameHeader(uint32(length), 0, 0, 0, 0))

	msg.SetHeaders(&headers)
	return &msg
}