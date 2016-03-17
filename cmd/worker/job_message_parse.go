package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/awslabs/aws-go-wordfreq-sample"
)

// parseJobMessage unmarshals the JSON job message, and constructs a worker job
// from it. Since S3 messages can include multiple records each individual job
// is added to the job channel so a worker from the worker pool can read it,
// and process the job.
func parseJobMessage(jobCh chan<- *wordfreq.Job, msg wordfreq.JobMessage, timeout int64) error {
	fmt.Println("Procesing message", msg.ID)

	s3msg := s3EventMsg{}
	if err := json.Unmarshal([]byte(msg.Body), &s3msg); err != nil {
		return fmt.Errorf("parse Amazon S3 Event message %v", err)
	}

	if len(s3msg.Records) == 0 {
		return fmt.Errorf("job does not have any records")
	}

	for _, record := range s3msg.Records {
		jobCh <- &wordfreq.Job{
			StartedAt:         time.Now(),
			VisibilityTimeout: timeout,
			OrigMessage:       msg,
			Region:            record.Region,
			Bucket:            record.S3.Bucket.Name,
			Key:               record.S3.Object.Key,
		}
	}

	return nil
}

// A s3EventMsg represents the SQS message provided by S3 Notifications. This
// is an abbreviated form of the message since not all fields are used by this
// service.
type s3EventMsg struct {
	Event   string
	Records []struct {
		Region    string `json:"awsRegion"`
		EventName string
		S3        struct {
			Bucket struct {
				Name string
			}
			Object struct {
				Key string
			}
		}
	}
}
