package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/awslabs/aws-go-wordfreq-sample"
)

func main() {
	doneCh := listenForSigInterrupt()

	cfg, err := getConfig()
	if err != nil {
		log.Println("Unable to get config", err)
		os.Exit(1)
	}

	sqsSvc := sqs.New(nil)
	queue := NewJobMessageQueue(cfg.WorkerQueueURL, cfg.MessageVisibilityTimeout, 5, sqsSvc)
	go queue.Listen(doneCh)

	// Job Workers
	resultsCh := make(chan *wordfreq.JobResult, 10)
	workers := NewWorkerPool(cfg.NumWorkers, resultsCh, queue, s3.New(nil))

	// Notifier to notify a Amazon SNS Topic
	notify := NewResultNotifier(sqsSvc, cfg.ResultQueueURL)
	// Recorder to write results to Amazon DynamoDB
	recorder := NewResultRecorder(cfg.ResultTableName, dynamodb.New(nil))

	// Job Progress Collector
	collector := NewResultCollector(notify, recorder, queue)
	go collector.ProcessJobResult(resultsCh)

	// Wait for the workers to complete before continuing on to exit
	workers.WaitForWorkersDone()
	close(resultsCh)

	// Wait for all results to be completed before continuing
	collector.WaitForResults()
}

// Handle Ctr+C
func listenForSigInterrupt() <-chan struct{} {
	doneCh := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		closed := false
		for sig := range sigCh {
			if !closed {
				fmt.Printf("Received %s, existing...\n", sig)
				closed = true
				close(doneCh)
			}
		}
	}()

	return doneCh
}
