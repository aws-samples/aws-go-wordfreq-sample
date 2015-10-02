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

type JobMessageQueue struct {
	queueURL        string
	queueVisibility int64
	queueWait       int64

	jobCh  chan *wordfreq.Job
	msgSvc sqsiface.SQSAPI
}

func NewJobMessageQueue(url string, visibilityTime, waitTime int64, svc sqsiface.SQSAPI) *JobMessageQueue {
	return &JobMessageQueue{
		queueURL:        url,
		queueVisibility: visibilityTime,
		queueWait:       waitTime,
		jobCh:           make(chan *wordfreq.Job, 10),
		msgSvc:          svc,
	}
}

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

			for _, msg := range msgs {
				parseJobMessage(m.jobCh,
					wordfreq.JobMessage{
						ID:            *msg.MessageId,
						ReceiptHandle: *msg.ReceiptHandle,
						Body:          *msg.Body,
					},
					m.queueVisibility,
				)
			}
		}
	}
}

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

func (m *JobMessageQueue) DeleteMessage(receiptHandle string) error {
	_, err := m.msgSvc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(m.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

func (m *JobMessageQueue) UpdateMessageVisibility(receiptHandle string) (int64, error) {
	_, err := m.msgSvc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(m.queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: aws.Int64(m.queueVisibility),
	})
	return m.queueVisibility, err
}

func (m *JobMessageQueue) GetJobs() <-chan *wordfreq.Job {
	return m.jobCh
}
