package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/awslabs/aws-go-wordfreq-sample"
)

type ResultCollector struct {
	recorder *ResultRecorder
	notify   *ResultNotifier
	queue    *JobMessageQueue

	wg sync.WaitGroup
}

// NewResultCollector creates a new instance of the ProgressCollector
func NewResultCollector(notify *ResultNotifier, recorder *ResultRecorder, queue *JobMessageQueue) *ResultCollector {
	return &ResultCollector{
		notify:   notify,
		recorder: recorder,
		queue:    queue,
	}
}

// ProcessJobProgresses processes all jobs in job progress channel until channel
// is drained.
func (r *ResultCollector) ProcessJobResult(resultCh <-chan *wordfreq.JobResult) {
	r.wg.Add(1)
	fmt.Println("Job Result Collector starting.")
	defer fmt.Println("Job Result Collector quiting.")
	defer r.wg.Done()

	for {
		result, ok := <-resultCh
		if !ok {
			return
		}
		message := result.Job.OrigMessage
		fmt.Println("Recived job result", message.ID)

		if result.Status == wordfreq.JobCompleteSuccess {
			fmt.Println("Succesffuly processed job", message.ID)

			// Record result to dynamoDB, and delete message if successful
			// if the writing to dynamoDB fails, don't delete the message
			// so the job can be retried by another worker later.
			if err := r.recorder.Record(result); err != nil {
				result.Status = wordfreq.JobCompleteFailure
				result.StatusMessage = fmt.Sprintf("record results failed, %v", err)
				log.Println("failed to recored result", message.ID, err)
			} else {
				err := r.queue.DeleteMessage(message.ReceiptHandle)
				if err != nil {
					log.Println("Failed to delete message,", message.ID, err)
				}
				fmt.Println("Deleted message,", message.ID)
			}

		} else {
			log.Println("Failed to process job", message.ID)
		}

		if err := r.notify.Send(result); err != nil {
			log.Println("Failed to send result to SQS queue", err)
		}
	}
}

// WaitForCollector Wait for the collector to finish before returning
func (r *ResultCollector) WaitForResults() {
	r.wg.Wait()
}
