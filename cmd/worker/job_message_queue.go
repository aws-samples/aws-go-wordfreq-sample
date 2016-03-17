package main

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/awslabs/aws-go-wordfreq-sample"
)

// A JobMessageQueue provides listening to a SQS queue for job messages, and
// providing those job messages as a job channel to workers so the jobs can be
// processed.
type JobMessageQueue struct {
	queueURL        string
	queueVisibility int64
	queueWait       int64

	jobCh  chan *wordfreq.Job
	msgSvc sqsiface.SQSAPI
}

// NewJobMessageQueue creates a new instance of the JobMessageQueue configuring it
// for the SQS service client it will use. The sqsiface.SQSAPI is used so that
// the code could be unit tested in isolating without also testing the SDK.
func NewJobMessageQueue(url string, visibilityTime, waitTime int64, svc sqsiface.SQSAPI) *JobMessageQueue {
	return &JobMessageQueue{
		queueURL:        url,
		queueVisibility: visibilityTime,
		queueWait:       waitTime,
		jobCh:           make(chan *wordfreq.Job, 10),
		msgSvc:          svc,
	}
}

// Listen waits for messages to arrive from the SQS queue, parses the JSON
// message and sends the jobs to the job channel to be processed by the worker pool.
func (m *JobMessageQueue) Listen(doneCh <-chan struct{}) {
	fmt.Println("Job Message queue starting")
	defer close(m.jobCh)
	defer fmt.Println("Job Message queue quitting.")

	for {
		select {
		case <-doneCh:
			return
		default:
			msgs, err := m.receiveMsg()
			if err != nil {
				log.Println("Failed to read from message queue", err)
				time.Sleep(5 * time.Second)
				continue
			}

			// Since SQS ReceiveMessage could return multiple messages at once
			// we should loop over then instead of assuming only a single message
			// message is returned. This is also and easier pattern if we want
			// to bump up the number of messages that will be read from SQS at once
			// by default only one message is read.
			for _, msg := range msgs {
				parseErr := parseJobMessage(m.jobCh,
					wordfreq.JobMessage{
						ID:            *msg.MessageId,
						ReceiptHandle: *msg.ReceiptHandle,
						Body:          *msg.Body,
					},
					m.queueVisibility,
				)
				if parseErr != nil {
					fmt.Println("Failed to parse", *msg.MessageId, "job message,", parseErr)
					m.DeleteMessage(*msg.ReceiptHandle)
				}
			}
		}
	}
}

// receiveMsg reads a message from the SQS job queue. A visibility timeout is set
// so that no other reader will be able to see the message which this service
// received. Preventing duplication of work. And a wait time provides long pooling
// so the service does not need to micro manage its pooling of SQS.
func (m *JobMessageQueue) receiveMsg() ([]*sqs.Message, error) {
	result, err := m.msgSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:          aws.String(m.queueURL),
		WaitTimeSeconds:   aws.Int64(m.queueWait),
		VisibilityTimeout: aws.Int64(m.queueVisibility),
	})
	if err != nil {
		return nil, err
	}

	return result.Messages, nil
}

// DeleteMessage deletes a previously received message from the job message queue
// Once a job is complete it can safely be deleted from the queue so that no
// other service or worker will rerun the job.
func (m *JobMessageQueue) DeleteMessage(receiptHandle string) error {
	_, err := m.msgSvc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(m.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

// UpdateMessageVisibility extends the amount of time a job message is hidden from
// other readers of the SQS job queue. This allows a worker to keep processing
// a long running job.
func (m *JobMessageQueue) UpdateMessageVisibility(receiptHandle string) (int64, error) {
	_, err := m.msgSvc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(m.queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: aws.Int64(m.queueVisibility),
	})
	return m.queueVisibility, err
}

// GetJobs returns a read only channel to read jobs from. This channel will
// be closed when the JobMessageQueue no longer is listening for further SQS
// job messages.
func (m *JobMessageQueue) GetJobs() <-chan *wordfreq.Job {
	return m.jobCh
}
