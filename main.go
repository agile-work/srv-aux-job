package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/agile-work/srv-aux-job/controllers"
	shared "github.com/agile-work/srv-shared"
	"github.com/agile-work/srv-shared/amqp"
	"github.com/agile-work/srv-shared/sql-builder/db"
)

var (
	jobInstanceName        = flag.String("name", "Job", "Name of this instance")
	jobConcurrencyWorkers  = flag.Int("jobs", 3, "Number of job processing concurrency")
	taskConcurrencyWorkers = flag.Int("taks", 3, "Number of tasks processing concurrency")
	heartbeatInterval      = flag.Int("heartbeat", 10, "Number of seconds to send a heartbeat")
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

	flag.Parse()
	fmt.Println("Starting Service...")
	err := db.Connect(host, port, user, password, dbName, false)
	if err != nil {
		fmt.Println("Error connecting to database")
		return
	}
	fmt.Println("Database connected")

	service, err := shared.RegisterService(*jobInstanceName, shared.ServiceTypeAuxiliary)
	if err != nil {
		fmt.Println("Error registering service in the database")
		return
	}
	fmt.Printf("Service %s registered\n", service.ID)

	jobMessages := make(chan *amqp.Message)

	//TODO: Load system params
	systemParams := make(map[string]string)
	systemParams["api_host"] = "https://localhost:8080"

	for w := 1; w <= *jobConcurrencyWorkers; w++ {
		job := &controllers.Job{
			Instance:     w,
			Concurrency:  *taskConcurrencyWorkers,
			Execution:    make(chan *controllers.Task, 100),
			Responses:    make(chan *controllers.Task, 100),
			SystemParams: systemParams,
		}
		pool = append(pool, job)
		go job.Process(jobMessages, service.ID)
	}

	jobsQueue, _ := amqp.New("amqp://guest:guest@localhost:5672/", "jobs", false)

	msgs, _ := jobsQueue.Stream()

	go func() {
		for d := range msgs {
			jobMessages <- amqp.Parse(d.Body)
			d.Ack(true)
		}
	}()

	ticker := time.NewTicker(time.Duration(*heartbeatInterval) * time.Second)
	go func() {
		for t := range ticker.C {
			service.Heartbeat(t)
		}
	}()

	jobsQueue.Push(amqp.Message{
		ID:    "4b8948b1-6778-47ca-9c5b-f621985d3ceb",
		Queue: "jobs",
	})

	<-stopChan
	fmt.Println("Shutting down Service...")

	service.Down()

	amqp.Close()
	db.Close()
	ticker.Stop()
	//TODO check if jobsQueue.Stream() is closed before close jobMessage channel
	//TODO check if there is a job being executed and wait before exit
	close(jobMessages)
	fmt.Println("Service stopped!")

}
