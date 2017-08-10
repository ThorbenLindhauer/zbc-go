package main

import (
	"encoding/json"
	"fmt"
	"github.com/zeebe-io/zbc-go/zbc"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

var (
	ProcessedEventsCount uint64
	ErrorCount           uint64
)

type StopCh chan bool

var workers map[string]StopCh

func processTask(lo string, msg *zbc.Message) {
	log.Printf("[%s] Working on task.\n", lo)
}

func openSubscription(client *zbc.Client, stopCh chan bool, pid int32, topic string, lo string, tt string) {
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
	if err != nil {
		atomic.AddUint64(&ErrorCount, 1)
	}

	log.Println("Subscription opened.")
	log.Println("Waiting for events ....")
	for {
		select {
		case message := <-subscriptionCh:
			processTask(lo, message)

			completeTaskMsg := zbc.NewCompleteTaskMessage(message)
			response, err := client.Responder(completeTaskMsg)

			if err != nil {
				log.Println("Completing a task went wrong.")
				log.Println(err)
			}
			if (*response.Data)["state"] == "COMPLETED" {
				atomic.AddUint64(&ProcessedEventsCount, 1)
				log.Println("Task completed successfully.")
			} else {
				log.Println("Task not completed.")
			}
			break

		case stop := <-stopCh:
			if stop == true {
				log.Print("Stopping worker.")
				_, err := client.Responder(zbc.NewCloseTaskSubscriptionMessage(taskSub))
				if err != nil {
					log.Println("Close task subscription request failed")
				}
				log.Println("Gracefully shutting down the client.")
				client.Close()
				return
			}
			break
		}
	}
}

func startWorkerView(w http.ResponseWriter, r *http.Request) {
	resp := make(map[string]interface{})
	zbClient, err := zbc.NewClient("192.168.21.72:51015")
	if err != nil {
		resp["status"] = http.StatusInternalServerError
	}
	resp["status"] = http.StatusOK
	lockOwner := fmt.Sprintf("zbc-%s", time.Now().Format("20060102150405"))

	log.Printf("Starting worker with ID: %s\n", lockOwner)
	workers[lockOwner] = make(chan bool)
	go openSubscription(zbClient, workers[lockOwner], 0, "default-topic", lockOwner, "foo")

	resp["workerID"] = lockOwner
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Write(jsonResp)
}

func statsView(w http.ResponseWriter, r *http.Request) {
	resp := make(map[string]interface{})
	resp["Running"] = true
	if len(workers) == 0 {
		resp["Running"] = false
	} else {
		resp["WorkersRunning"] = len(workers)
		resp["ProcessedEventsCount"] = atomic.LoadUint64(&ProcessedEventsCount)
		resp["ErrorCount"] = atomic.LoadUint64(&ErrorCount)

		var workersIDs []string
		for key, _ := range workers {
			workersIDs = append(workersIDs, key)
		}
		resp["WorkersIDs"] = workersIDs
	}
	jsonResp, err := json.MarshalIndent(resp, "", "    ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Write(jsonResp)
}

func stopWorkersView(w http.ResponseWriter, r *http.Request) {
	resp := make(map[string]interface{})
	for _, worker := range workers {
		worker <- true
	}
	workers = make(map[string]StopCh)
	resp["Status"] = http.StatusOK

	jsonResp, err := json.MarshalIndent(resp, "", "    ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Write(jsonResp)
}

func main() {
	workers = make(map[string]StopCh)
	log.Println("Super microservice started.")
	log.Println("Waiting for workers to start.")

	http.HandleFunc("/start", startWorkerView)
	http.HandleFunc("/stop", stopWorkersView)
	http.HandleFunc("/stats", statsView)
	// TODO: implement increase credits

	http.ListenAndServe(":3000", nil)
}
