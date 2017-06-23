package zbc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/jsam/zbc-go/zbc/protocol"
	"github.com/jsam/zbc-go/zbc/sbe"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
)

var (
	FrameHeaderReadError   = errors.New("Cannot read bytes for frame header.")
	FrameHeaderDecodeError = errors.New("Cannot decode bytes into frame header.")
	ProtocolIdNotFound     = errors.New("ProtocolId not found.")
)

type MessageReader struct {
	io.Reader
}

func (mr *MessageReader) readNext(n uint32) ([]byte, error) {
	buffer := make([]byte, n)

	numBytes, err := mr.Read(buffer)

	if uint32(numBytes) != n || err != nil {
		return nil, err //MessageBinaryReadError
	}

	return buffer, nil
}

func (mr *MessageReader) readFrameHeader(data []byte) (*protocol.FrameHeader, error) {
	var frameHeader protocol.FrameHeader
	if frameHeader.Decode(bytes.NewReader(data[:12]), binary.LittleEndian, 0) != nil {
		return nil, FrameHeaderDecodeError
	}
	return &frameHeader, nil
}

func (mr *MessageReader) readTransportHeader(data []byte) (*protocol.TransportHeader, error) {
	var transport protocol.TransportHeader
	err := transport.Decode(bytes.NewReader(data[:2]), binary.LittleEndian, 0)
	if err != nil {
		return nil, err
	}
	if transport.ProtocolId == protocol.RequestResponse || transport.ProtocolId == protocol.FullDuplexSingleMessage {
		return &transport, nil
	}

	return nil, ProtocolIdNotFound
}

func (mr *MessageReader) readRequestResponseHeader(data []byte) (*protocol.RequestResponseHeader, error) {
	var requestResponse protocol.RequestResponseHeader
	err := requestResponse.Decode(bytes.NewReader(data[:16]), binary.LittleEndian, 0)
	if err != nil {
		return nil, err
	}

	return &requestResponse, nil
}

func (mr *MessageReader) readSbeMessageHeader(data []byte) (*sbe.MessageHeader, error) {
	var sbeMessageHeader sbe.MessageHeader
	err := sbeMessageHeader.Decode(bytes.NewReader(data[:8]), binary.LittleEndian, 0)
	if err != nil {
		return nil, err
	}
	return &sbeMessageHeader, nil
}

func (mr *MessageReader) ReadHeaders() (*Headers, *[]byte, error) {
	var header Headers

	headerByte, err := mr.readNext(12)
	if err != nil {
		return nil, nil, FrameHeaderReadError
	}

	frameHeader, err := mr.readFrameHeader(headerByte)
	if err != nil {
		return nil, nil, err
	}
	header.SetFrameHeader(frameHeader)

	message, err := mr.readNext(frameHeader.Length)
	if err != nil {
		return nil, nil, err
	}

	transport, err := mr.readTransportHeader(message[:2])
	if err != nil {
		return nil, nil, err
	}
	header.SetTransportHeader(transport)

	sbeIndex := 2
	switch transport.ProtocolId {
	case protocol.RequestResponse:
		requestResponse, err := mr.readRequestResponseHeader(message[2:18])
		if err != nil {
			return nil, nil, err
		}
		header.SetRequestResponseHeader(requestResponse)
		sbeIndex = 18
		break

	case protocol.FullDuplexSingleMessage:
		header.SetRequestResponseHeader(nil)
		break
	}

	sbeMessageHeader, err := mr.readSbeMessageHeader(message[sbeIndex : sbeIndex+8])
	if err != nil {
		return nil, nil, err
	}
	header.SetSbeMessageHeader(sbeMessageHeader)

	// this should align the reader for the next message
	mr.align()

	body := message[sbeIndex+8:]
	return &header, &body, nil
}

func (mr *MessageReader) align() {
	// TODO:
}

func (mr *MessageReader) decodeCmdRequest(reader *bytes.Reader, header *sbe.MessageHeader) (*sbe.ExecuteCommandRequest, error) {
	var commandRequest sbe.ExecuteCommandRequest
	err := commandRequest.Decode(reader,
		binary.LittleEndian,
		header.Version,
		header.BlockLength,
		true)
	if err != nil {
		return nil, err
	}
	return &commandRequest, nil
}

func (mr *MessageReader) decodeCmdResponse(reader *bytes.Reader, header *sbe.MessageHeader) (*sbe.ExecuteCommandResponse, error) {
	var commandResponse sbe.ExecuteCommandResponse
	err := commandResponse.Decode(reader,
		binary.LittleEndian,
		header.Version,
		header.BlockLength,
		true)
	if err != nil {
		return nil, err
	}
	return &commandResponse, nil
}

func (mr *MessageReader) decodeCtlResponse(reader *bytes.Reader, header *sbe.MessageHeader) (*sbe.ControlMessageResponse, error) {
	var controlResponse sbe.ControlMessageResponse
	err := controlResponse.Decode(reader, binary.LittleEndian, header.Version, header.BlockLength, true)
	if err != nil {
		return nil, err
	}
	return &controlResponse, nil
}

func (mr *MessageReader) decodeSubEvent(reader *bytes.Reader, header *sbe.MessageHeader) (*sbe.SubscribedEvent, error) {
	var subEvent sbe.SubscribedEvent
	err := subEvent.Decode(reader, binary.LittleEndian, header.Version, header.BlockLength, true)
	if err != nil {
		return nil, err
	}
	return &subEvent, nil
}

func (mr *MessageReader) parseMessagePack(data *[]byte) (*map[string]interface{}, error) {
	var item map[string]interface{}
	err := msgpack.Unmarshal(*data, &item)

	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (mr *MessageReader) ParseMessage(headers *Headers, message *[]byte) (*Message, error) {
	var msg Message
	msg.SetHeaders(headers)
	reader := bytes.NewReader(*message)

	switch headers.SbeMessageHeader.TemplateId {

	case SBE_ExecuteCommandRequest_TemplateId: // Testing purposes.
		commandRequest, err := mr.decodeCmdRequest(reader, headers.SbeMessageHeader)
		if err != nil {
			return nil, err
		}
		msg.SetSbeMessage(commandRequest)

		msgPackData, err := mr.parseMessagePack(&commandRequest.Command)
		if err != nil {
			return nil, err
		}

		msg.SetData(msgPackData)
		break

	case SBE_ExecuteCommandResponse_TemplateId: // Read response from the socket.
		commandResponse, err := mr.decodeCmdResponse(reader, headers.SbeMessageHeader)
		if err != nil {
			return nil, err
		}
		msg.SetSbeMessage(commandResponse)

		msgPackData, err := mr.parseMessagePack(&commandResponse.Event)
		if err != nil {
			return nil, err
		}
		msg.SetData(msgPackData)
		break

	case SBE_ControlMessage_Response_TemplateId:
		ctlResponse, err := mr.decodeCtlResponse(reader, headers.SbeMessageHeader)
		if err != nil {
			return nil, err
		}
		msg.SetSbeMessage(ctlResponse)
		msgPackData, err := mr.parseMessagePack(&ctlResponse.Data)
		if err != nil {
			return nil, err
		}
		msg.SetData(msgPackData)
		break

	case SBE_SubscriptionEvent_TemplateId:
		subscribedEvent, err := mr.decodeSubEvent(reader, headers.SbeMessageHeader)
		if err != nil {
			return nil, err
		}

		msg.SetSbeMessage(subscribedEvent)
		msgPackData, err := mr.parseMessagePack(&subscribedEvent.Event)
		if err != nil {
			return nil, err
		}
		msg.SetData(msgPackData)
		break
	}
	return &msg, nil
}

func NewMessageReader(rd *bufio.Reader) *MessageReader {
	return &MessageReader{
		rd,
	}
}
