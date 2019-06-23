package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/agile-work/srv-shared/rdb"

	"github.com/agile-work/srv-aux-job/controllers"
	"github.com/agile-work/srv-shared/constants"
	"github.com/agile-work/srv-shared/socket"
	"github.com/agile-work/srv-shared/sql-builder/db"
	"github.com/agile-work/srv-shared/util"
)

var (
	serviceInstanceName    = flag.String("name", "Job", "Name of this instance")
	jobConcurrencyWorkers  = flag.Int("jobs", 3, "Number of job processing concurrency")
	taskConcurrencyWorkers = flag.Int("taks", 3, "Number of tasks processing concurrency")
	host                   = "cryo.cdnm8viilrat.us-east-2.rds-preview.amazonaws.com"
	port                   = 5432
	user                   = "cryoadmin"
	password               = "x3FhcrWDxnxCq9p"
	dbName                 = "cryo"
	redisHost              = flag.String("redisHost", "localhost", "Redis host")
	redisPort              = flag.Int("redisPort", 6379, "Redis port")
	redisPass              = flag.String("redisPass", "redis123", "Redis password")
	wsHost                 = flag.String("wsHost", "localhost", "Realtime host")
	wsPort                 = flag.Int("wsPort", 8010, "Realtime port")
)

var pool []*controllers.Job

func main() {
	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, os.Interrupt)

	flag.Parse()
	fmt.Printf("Starting Service %s...\n", *serviceInstanceName)
	fmt.Println("Database connecting...")
	err := db.Connect(host, port, user, password, dbName, false)
	if err != nil {
		fmt.Println("Error connecting to database")
		return
	}
	fmt.Println("Database connected")

	rdb.Init(*redisHost, *redisPort, *redisPass)
	defer rdb.Close()

	socket.Init(*serviceInstanceName, constants.ServiceTypeAuxiliary, *wsHost, *wsPort)
	defer socket.Close()

	jobMessages := make(chan string)

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
		go job.Process(jobMessages, *serviceInstanceName)
	}

	jobInstanceQueue := fmt.Sprintf("worker:%s", *serviceInstanceName)
	shutdown := false

	go func() {
		jobInstanceQueueRetry := false
		for {
			if rdb.Available() && !shutdown {
				err := rdb.BRPopLPush("queue:jobs", jobInstanceQueue, 0)
				if err == nil || jobInstanceQueueRetry {
					msg, err := rdb.LPop(jobInstanceQueue)
					if err != nil {
						jobInstanceQueueRetry = true
						continue
					}
					jobMessages <- msg
					jobInstanceQueueRetry = false
				}
			}
		}
	}()

	fmt.Printf("Job pid:%d ready...\n", os.Getpid())

	<-stopChan
	fmt.Println("\nShutting down Service...")
	shutdown = true
	if len(jobMessages) > 0 {
		fmt.Printf("%d unprocessed jobs\n", len(jobMessages))
	}
	close(jobMessages)
	for poolProcessing() {
		time.Sleep(2 * time.Second)
	}
	db.Close()
	fmt.Println("Service stopped!")
}

func poolProcessing() bool {
	for _, p := range pool {
		if p.Processing {
			return true
		}
	}
	return false
}
