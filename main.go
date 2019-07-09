package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/agile-work/srv-shared/rdb"
	"github.com/agile-work/srv-shared/service"
	"github.com/agile-work/srv-shared/token"

	"github.com/agile-work/srv-aux-job/controllers"
	"github.com/agile-work/srv-shared/constants"
	"github.com/agile-work/srv-shared/socket"
	"github.com/agile-work/srv-shared/sql-builder/db"
	"github.com/agile-work/srv-shared/util"
)

var (
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

	pid := os.Getpid()
	hostname, _ := os.Hostname()
	aux := service.New("Job", constants.ServiceTypeAuxiliary, hostname, 0, pid)

	fmt.Printf("Starting Service %s...\n", aux.Name)
	fmt.Printf("[Instance: %s | PID: %d]\n", aux.InstanceCode, aux.PID)

	fmt.Println("Database connecting...")
	err := db.Connect(host, port, user, password, dbName, false)
	if err != nil {
		fmt.Println("Error connecting to database")
		return
	}
	fmt.Println("Database connected")

	rdb.Init(*redisHost, *redisPort, *redisPass)
	defer rdb.Close()

	socket.Init(aux, *wsHost, *wsPort)
	defer socket.Close()

	jobMessages := make(chan string)

	systemParams, err := util.GetSystemParams()
	if err != nil {
		// TODO: Pensar em como tratar esse erro
		fmt.Println(err.Error())
	}

	payload := make(map[string]interface{})
	payload["code"] = systemParams[constants.SysParamAPIUsername]
	payload["language_code"] = systemParams[constants.SysParamDefaultLanguageCode]

	tokenString, err := token.New(payload, constants.Year)
	if err != nil {
		fmt.Printf("Error creating token - %s\n", err.Error())
		return
	}

	for w := 1; w <= *jobConcurrencyWorkers; w++ {
		job := &controllers.Job{
			Instance:     w,
			Concurrency:  *taskConcurrencyWorkers,
			Execution:    make(chan *controllers.Task, 100),
			Responses:    make(chan *controllers.Task, 100),
			SystemParams: systemParams,
			Token:        tokenString,
		}
		pool = append(pool, job)
		go job.Process(jobMessages, aux.InstanceCode)
	}

	jobInstanceQueue := fmt.Sprintf("worker:%s", aux.InstanceCode)
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

	fmt.Println("Job ready...")

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
