package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/agile-work/srv-aux-job/controllers"
	"github.com/agile-work/srv-shared/amqp"
	"github.com/agile-work/srv-shared/sql-builder/db"
)

var (
	jobConcurrencyWorkers  = flag.Int("job-workers", 3, "Number of job processing concurrency")
	taskConcurrencyWorkers = flag.Int("taks-workers", 3, "Number of tasks processing concurrency")
	host                   = "cryo.cdnm8viilrat.us-east-2.rds-preview.amazonaws.com"
	port                   = 5432
	user                   = "cryoadmin"
	password               = "x3FhcrWDxnxCq9p"
	dbName                 = "cryo"
)

var pool []*controllers.Job

func main() {
	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, os.Interrupt)

	err := db.Connect(host, port, user, password, dbName, false)
	if err != nil {
		fmt.Println("Error connecting to database")
		return
	}

	flag.Parse()
	//TODO: register service in the database, get serviceID, and start heartbeat
	serviceID := "00001"

	jobMessages := make(chan *amqp.Message)

	for w := 1; w <= *jobConcurrencyWorkers; w++ {
		job := &controllers.Job{
			ServiceID:   serviceID,
			Instance:    w,
			Concurrency: *taskConcurrencyWorkers,
			Execution:   make(chan *controllers.Task),
			Responses:   make(chan *controllers.Task),
		}
		pool = append(pool, job)
		go job.Process(jobMessages)
	}

	jobsQueue, _ := amqp.New("amqp://guest:guest@localhost:5672/", "jobs", false)

	msgs, _ := jobsQueue.Stream()

	go func() {
		for d := range msgs {
			jobMessages <- amqp.Parse(d.Body)
			d.Ack(true)
		}
	}()

	m := amqp.Message{
		ID:    "000001",
		Queue: "jobs",
	}
	jobsQueue.Push(m)

	<-stopChan
	fmt.Println("Shutting down Service...")
	amqp.Close()
	//TODO check if jobsQueue.Stream() is closed before close jobMessage channel
	close(jobMessages)
	fmt.Println("Service stopped!")

}
