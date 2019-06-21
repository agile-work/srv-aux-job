package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/agile-work/srv-aux-job/controllers"
	"github.com/agile-work/srv-shared/amqp"
	"github.com/agile-work/srv-shared/constants"
	"github.com/agile-work/srv-shared/service"
	"github.com/agile-work/srv-shared/sql-builder/db"
	"github.com/agile-work/srv-shared/util"
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

	jobsQueue, err := amqp.New("amqp://guest:guest@localhost:5672/", "jobs", false)
	if err != nil {
		fmt.Println("Error connecting to queue")
		return
	}
	fmt.Println("Queue connected")

	srv, err := service.Register(*jobInstanceName, constants.ServiceTypeAuxiliary)
	if err != nil {
		fmt.Println("Error registering service in the database")
		return
	}
	// TODO: fix
	// fmt.Printf("Service %s registered\n", service.ID)

	jobMessages := make(chan *amqp.Message)

	systemParams, err := util.GetSystemParams()
	if err != nil {
		// TODO: Pensar em como tratar esse erro
		fmt.Println(err.Error())
	}

	for w := 1; w <= *jobConcurrencyWorkers; w++ {
		job := &controllers.Job{
			Instance:     w,
			Concurrency:  *taskConcurrencyWorkers,
			Execution:    make(chan *controllers.Task, 100),
			Responses:    make(chan *controllers.Task, 100),
			SystemParams: systemParams,
		}
		pool = append(pool, job)
		// TODO: fix
		// go job.Process(jobMessages, service.ID)
	}

	msgs, _ := jobsQueue.Stream()

	go func() {
		for d := range msgs {
			jobMessages <- amqp.Parse(d.Body)
			d.Ack(true)
		}
	}()

	<-stopChan
	fmt.Println("Shutting down Service...")

	srv.Down()

	amqp.Close()
	db.Close()
	// TODO: fix
	// ticker.Stop()
	//TODO check if jobsQueue.Stream() is closed before close jobMessage channel
	//TODO check if there is a job being executed and wait before exit
	close(jobMessages)
	fmt.Println("Service stopped!")

}
