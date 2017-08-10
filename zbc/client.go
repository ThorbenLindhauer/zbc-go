package zbc

import (
	"bufio"
	"bytes"
	"errors"
	"log"
	"net"
	"time"

	"github.com/zeebe-io/zbc-go/zbc/sbe"
)

// RequestTimeout specifies default timeout for Responder.
const RequestTimeout = 5

var (
	errTimeout     = errors.New("Request timeout")
	errSocketWrite = errors.New("Tried to write more bytes to socket")
)

// Client for one Zeebe broker
type Client struct {
	conn          net.Conn
	isActive     bool
	transactions  map[uint64]chan *Message
	subscriptions map[uint64]chan *Message
}

func (c *Client) sender(message *Message) error {
	writer := NewMessageWriter(message)
	byteBuff := &bytes.Buffer{}
	writer.Write(byteBuff)

	n, err := c.conn.Write(byteBuff.Bytes())
	if err != nil {
		return err
	}

	if n != len(byteBuff.Bytes()) {
		return errSocketWrite
	}
	return nil
}

func (c *Client) receiver() {
	for {
		if !c.isActive {
			c.conn.Close()
			return
		}
		buffer := bufio.NewReaderSize(c.conn, 20000)
		r := NewMessageReader(buffer)
		headers, tail, err := r.ReadHeaders()
		if err != nil {
			log.Printf("[R] Error %+#v\n", err)
			continue
		}
		message, err := r.ParseMessage(headers, tail)

		if err != nil && !headers.IsSingleMessage() {
			// TODO: Maybe we should panic here?
			delete(c.transactions, headers.RequestResponseHeader.RequestID)
			continue
		}

		if !headers.IsSingleMessage() && message != nil {
			c.transactions[headers.RequestResponseHeader.RequestID] <- message
			continue
		}

		if err != nil && headers.IsSingleMessage() {
			// TODO: close and delete subscription
			continue
		}

		if headers.IsSingleMessage() && message != nil {
			subscriberKey := (*message.SbeMessage).(*sbe.SubscribedEvent).SubscriberKey
			c.subscriptions[subscriberKey] <- message
			continue
		}

	}
}

// Responder implements synchronous way of sending ExecuteCommandRequest and waiting for ExecuteCommandResponse.
func (c *Client) Responder(message *Message) (*Message, error) {
	respCh := make(chan *Message)
	c.transactions[message.Headers.RequestResponseHeader.RequestID] = respCh

	if err := c.sender(message); err != nil {
		return nil, err
	}

	select {
	case resp := <-c.transactions[message.Headers.RequestResponseHeader.RequestID]:
		delete(c.transactions, message.Headers.RequestResponseHeader.RequestID)
		return resp, nil
	case <-time.After(time.Second * RequestTimeout):
		delete(c.transactions, message.Headers.RequestResponseHeader.RequestID)
		return nil, errTimeout
	}
}

// TaskConsumer opens a subscription on task and returns a channel where all the SubscribedEvents will arrive.
func (c *Client) TaskConsumer(ts *TaskSubscription) (chan *Message, error) {
	subscriptionCh := make(chan *Message, ts.Credits)
	msg := NewTaskSubscriptionMessage(ts)

	response, err := c.Responder(msg)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	subscriberKey := (*response.Data)["subscriberKey"].(uint64)
	log.Println(subscriberKey)
	c.subscriptions[subscriberKey] = subscriptionCh

	return subscriptionCh, nil
}

// Start will spinoff receiver in goroutine, which will make client effectively ready to communicate with the broker.
func (c *Client) Start() {
	c.isActive = true
	go c.receiver()
}

func (c *Client) Close() {
	c.isActive = false
}

// NewClient is constructor for Client structure. It will resolve IP address and dial the provided tcp address.
func NewClient(addr string) (*Client, error) {
	tcpAddr, wrongAddr := net.ResolveTCPAddr("tcp4", addr) // TODO: support IPv6 and TLS
	if wrongAddr != nil {
		return nil, wrongAddr
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}

	c := &Client{
		conn,
		false,
		make(map[uint64]chan *Message),
		make(map[uint64]chan *Message),
	}
	c.Start()

	return c, nil
}
