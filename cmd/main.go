package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/jsam/zbc-go/zbc"
	"github.com/jsam/zbc-go/zbc/sbe"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

const (
	version              = "0.1.0-alpha1"
	defaultConfiguration = "/etc/zeebe/config.toml"
)

var (
	errResourceNotFound = errors.New("Resource at the given path not found")
)

func isFatal(err error) {
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

type contact struct {
	Address string `toml:"address"`
	Port    string `toml:"port"`
}

func (c *contact) String() string {
	return fmt.Sprintf("%s:%s", c.Address, c.Port)
}

type config struct {
	Version string  `toml:"version"`
	Broker  contact `toml:"broker"`
}

func (cf *config) String() string {
	return fmt.Sprintf("version: %s\tBroker: %s", cf.Version, cf.Broker.String())

}

func sendCreateTask(client *zbc.Client, topic string, m *zbc.Task) (*zbc.Message, error) {
	commandRequest := zbc.NewTaskMessage(&sbe.ExecuteCommandRequest{
		PartitionId: 0,
		Key:         0,
		EventType:   sbe.EventTypeEnum(0),
		TopicName:   []uint8(topic),
		Command:     []uint8{},
	}, m)

	response, err := client.Responder(commandRequest)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return response, nil
}

func openSubscription(client *zbc.Client, topic string, pid int32, lo string, tt string) {
	taskSub := &zbc.TaskSubscription{
		TopicName:     topic,
		PartitionID:   pid,
		Credits:       32,
		LockDuration:  300000,
		LockOwner:     lo,
		SubscriberKey: 0,
		TaskType:      tt,
	}
	subscriptionCh, err := client.TaskConsumer(taskSub)
	isFatal(err)

	log.Println("Waiting for events ....")
	for {
		message := <-subscriptionCh
		fmt.Printf("%#v\n", *message.Data)
	}
}

func loadCommandYaml(path string) (*zbc.Task, error) {
	log.Printf("Loading resource at %s\n", path)
	if len(path) == 0 {
		return nil, errResourceNotFound
	}

	filename, _ := filepath.Abs(path)
	yamlFile, _ := ioutil.ReadFile(filename)

	var command zbc.Task
	err := yaml.Unmarshal(yamlFile, &command)
	if err != nil {
		return nil, err
	}
	return &command, nil
}

func loadConfig(path string, c *config) {
	if _, err := toml.DecodeFile(path, c); err != nil {
		log.Printf("Reading configuration failed. Expecting to found configuration file at %s\n", path)
		log.Printf("HINT: Configuration file is not in place. Try setting configuration path with:")
		log.Fatalln(" zbctl --config <path to config.toml>")
	}
}

func main() {
	var conf config

	app := cli.NewApp()
	app.Usage = "Zeebe control client application"
	app.Version = version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "config, cfg",
			Value:  defaultConfiguration,
			Usage:  "Location of the configuration file.",
			EnvVar: "ZBC_CONFIG",
		},
	}
	app.Before = cli.BeforeFunc(func(c *cli.Context) error {
		loadConfig(c.String("config"), &conf)
		log.Println(conf.String())
		return nil
	})

	app.Authors = []cli.Author{
		{Name: "Daniel Meyer", Email: ""},
		{Name: "Sebastian Menski", Email: ""},
		{Name: "Philipp Ossler", Email: ""},
		{Name: "Just Sam", Email: "samuel.picek@camunda.com"},
	}
	app.Commands = []cli.Command{
		{
			Name:    "create",
			Aliases: []string{"c"},
			Usage:   "create a resource",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "topic, t",
					Value:  "default-topic",
					Usage:  "Executing command request on specific topic.",
					EnvVar: "ZB_TOPIC_NAME",
				},
			},
			Action: func(c *cli.Context) error {
				createTask, err := loadCommandYaml(c.Args().First())
				isFatal(err)

				client, err := zbc.NewClient(conf.Broker.String())
				isFatal(err)
				log.Println("Connected to Zeebe.")

				response, err := sendCreateTask(client, c.String("topic"), createTask)
				isFatal(err)

				log.Println("Success. Received response:")
				log.Println(*response.Data)
				return nil
			},
		},
		{
			Name:    "open",
			Aliases: []string{"n"},
			Usage:   "open a subscription",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "topic, t",
					Value:  "default-topic",
					Usage:  "Executing command request on specific topic.",
					EnvVar: "ZB_TOPIC_NAME",
				},
				cli.Int64Flag{
					Name:   "partition-id, p",
					Value:  0,
					Usage:  "Specify partition on which we are opening subscription.",
					EnvVar: "ZB_PARTITION_ID",
				},
				cli.StringFlag{
					Name:   "lock-owner, l",
					Value:  "zbc",
					Usage:  "Specify lock owner.",
					EnvVar: "ZB_LOCK_OWNER",
				},
				cli.StringFlag{
					Name:   "task-type, tt",
					Value:  "foo",
					Usage:  "Specify task type.",
					EnvVar: "ZB_TASK_TYPE",
				},
			},
			Action: func(c *cli.Context) error {
				client, err := zbc.NewClient(conf.Broker.String())
				isFatal(err)
				log.Println("Connected to Zeebe.")
				openSubscription(client, c.String("topic"),
					int32(c.Int64("partition-id")),
					c.String("lock-owner"),
					c.String("task-type"))
				return nil
			},
		},
	}
	app.Run(os.Args)
}
