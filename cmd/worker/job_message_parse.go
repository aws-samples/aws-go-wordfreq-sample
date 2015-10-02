package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/awslabs/aws-go-wordfreq-sample"
)

func parseJobMessage(jobCh chan<- *wordfreq.Job, msg wordfreq.JobMessage, timeout int64) error {
	fmt.Println("Procesing message", msg.ID)

	s3msg := s3EventMsg{}
	if err := json.Unmarshal([]byte(msg.Body), &s3msg); err != nil {
		return fmt.Errorf("parse Amazon S3 Event message %v", err)
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

type s3EventMsg struct {
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
