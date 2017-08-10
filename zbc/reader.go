package zbc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/zeebe-io/zbc-go/zbc/protocol"
	"github.com/zeebe-io/zbc-go/zbc/sbe"
	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	errFrameHeaderRead    = errors.New("Cannot read bytes for frame header")
	errFrameHeaderDecode  = errors.New("Cannot decode bytes into frame header")
	errProtocolIDNotFound = errors.New("ProtocolId not found")
)

// MessageReader is builder which will read byte array and construct Message with all their parts.
type MessageReader struct {
	io.Reader
}

func (mr *MessageReader) readNext(n uint32) ([]byte, error) {
	buffer := make([]byte, n)

	numBytes, err := mr.Read(buffer)
	if uint32(numBytes) != n || err != nil {
		return nil, err
	}

	return buffer, nil
}

func (mr *MessageReader) readFrameHeader(data io.Reader) (*protocol.FrameHeader, error) {
	var frameHeader protocol.FrameHeader
	if frameHeader.Decode(data, binary.LittleEndian, 0) != nil {
		return nil, errFrameHeaderDecode
	}
	return &frameHeader, nil
}

func (mr *MessageReader) readTransportHeader(data io.Reader) (*protocol.TransportHeader, error) {
	var transport protocol.TransportHeader
	err := transport.Decode(data, binary.LittleEndian, 0)
	if err != nil {
		return nil, err
	}
	if transport.ProtocolID == protocol.RequestResponse || transport.ProtocolID == protocol.FullDuplexSingleMessage {
		return &transport, nil
	}

	return nil, errProtocolIDNotFound
}

func (mr *MessageReader) readRequestResponseHeader(data io.Reader) (*protocol.RequestResponseHeader, error) {
	var requestResponse protocol.RequestResponseHeader
	err := requestResponse.Decode(data, binary.LittleEndian, 0)
	if err != nil {
		return nil, err
	}

	return &requestResponse, nil
}

func (mr *MessageReader) readSbeMessageHeader(data io.Reader) (*sbe.MessageHeader, error) {
	var sbeMessageHeader sbe.MessageHeader
	err := sbeMessageHeader.Decode(data, binary.LittleEndian, 0)
	if err != nil {
		return nil, err
	}
	return &sbeMessageHeader, nil
}

// ReadHeaders will read entire message and interpret all headers. It will return pointer to headers object and tail of the message as byte array.
func (mr *MessageReader) ReadHeaders() (*Headers, *[]byte, error) {
	var header Headers

	headerByte, err := mr.readNext(FrameHeaderSize)
	if err != nil {
		return nil, nil, errFrameHeaderRead
	}

	frameHeaderReader := bytes.NewReader(headerByte)
	frameHeader, err := mr.readFrameHeader(frameHeaderReader)
	if err != nil {
		return nil, nil, err
	}
	header.SetFrameHeader(frameHeader)

	message, err := mr.readNext(frameHeader.Length)
	if err != nil {
		return nil, nil, err
	}

	transportReader := bytes.NewReader(message[:TransportHeaderSize])
	transport, err := mr.readTransportHeader(transportReader)
	if err != nil {
		return nil, nil, err
	}
	header.SetTransportHeader(transport)

	sbeIndex := TransportHeaderSize
	switch transport.ProtocolID {
	case protocol.RequestResponse:
		reqRespReader := bytes.NewReader(message[TransportHeaderSize : TransportHeaderSize+RequestResponseHeaderSize])
		requestResponse, errHeader := mr.readRequestResponseHeader(reqRespReader)
		if errHeader != nil {
			return nil, nil, err
		}
		header.SetRequestResponseHeader(requestResponse)
		sbeIndex = TransportHeaderSize + RequestResponseHeaderSize
		break

	case protocol.FullDuplexSingleMessage:
		header.SetRequestResponseHeader(nil)
		break
	}

	sbeHeaderReader := bytes.NewReader(message[sbeIndex : sbeIndex+SBEMessageHeaderSize])
	sbeMessageHeader, err := mr.readSbeMessageHeader(sbeHeaderReader)
	if err != nil {
		return nil, nil, err
	}
	header.SetSbeMessageHeader(sbeMessageHeader)

	body := message[sbeIndex+8:]
	return &header, &body, nil
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

// ParseMessage will take the headers and tail and construct Message.
func (mr *MessageReader) ParseMessage(headers *Headers, message *[]byte) (*Message, error) {
	var msg Message
	msg.SetHeaders(headers)
	reader := bytes.NewReader(*message)

	switch headers.SbeMessageHeader.TemplateId {

	case templateIDExecuteCommandRequest: // Testing purposes.
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

	case templateIDExecuteCommandResponse: // Read response from the socket.
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

	case templateIDControlMessageResponse:
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

	case templateIDSubscriptionEvent:
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

// NewMessageReader is constructor for MessageReader builder.
func NewMessageReader(rd *bufio.Reader) *MessageReader {
	return &MessageReader{
		rd,
	}
}
