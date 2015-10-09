package main

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/awslabs/aws-go-wordfreq-sample"
)

// A ResultNotifier provides pushing a result message to the SQS results queue.
type ResultNotifier struct {
	svc      sqsiface.SQSAPI
	queueURL string
}

// NewResultNotifier creates a new instance of the ResultNotifier type with the
// Amazon SQS service client and queue URL messages will sent to.
func NewResultNotifier(svc sqsiface.SQSAPI, queueURL string) *ResultNotifier {
	return &ResultNotifier{
		svc: svc, queueURL: queueURL,
	}
}

// Send sends a message to the Amazon SQS queue with the job's result.
func (r *ResultNotifier) Send(result *wordfreq.JobResult) error {
	msg, err := json.Marshal(result)
	if err != nil {
		return err
	}

	_, err = r.svc.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(r.queueURL),
		MessageBody: aws.String(string(msg)),
	})
	if err != nil {
		return err
	}
	return nil
}
