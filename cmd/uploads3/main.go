package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/awslabs/aws-go-wordfreq-sample"
)

// Uploads a file to S3 so it can be processed by the Word Frequency service.
// If a "WORKER_RESULT_QUEUE_URL" environment variable is provided the upload
// client will wait for the job to processed, and print the results to the console.
//
// Usage:
//  uploads3 <bucket> <filename>
func main() {
	if len(os.Args) != 3 {
		fmt.Printf("usage: %s <bucket> <filename>\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}
	bucket := os.Args[1]
	filename := os.Args[2]

	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Failed to open file", filename, err)
		os.Exit(1)
	}
	defer file.Close()

	fmt.Println("Uploading file to s3...")
	svc := s3manager.NewUploader(nil)
	result, err := svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filepath.Base(filename)),
		Body:   file,
		ACL:    aws.String("public-read"),
	})
	if err != nil {
		fmt.Println("error", err)
		os.Exit(1)
	}

	fmt.Printf("Succesfully uploaded %s to %s\n", filename, result.Location)

	if queueURL := os.Getenv("WORKER_RESULT_QUEUE_URL"); queueURL != "" {
		fmt.Println("Waiting for results...")
		waitForResult(bucket, filepath.Base(filename), queueURL)
	}
}

// waitForResult waits for the job to be processed and
func waitForResult(bucket, filename, resultQueueURL string) {
	svc := sqs.New(nil)
	for {
		resp, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:          aws.String(resultQueueURL),
			VisibilityTimeout: aws.Int64(0),
			WaitTimeSeconds:   aws.Int64(20),
		})
		if err != nil {
			log.Println("Failed to receive mesasge", err)
			time.Sleep(30 * time.Second)
			continue
		}

		for _, msg := range resp.Messages {
			result := &wordfreq.JobResult{}
			if err := json.Unmarshal([]byte(aws.StringValue(msg.Body)), result); err != nil {
				log.Println("Failed to unmarshal message", err)
				continue
			}

			if result.Job.Bucket != bucket || result.Job.Key != filename {
				continue
			}

			printResult(result)
			svc.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(resultQueueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
			return
		}
	}
}

func printResult(result *wordfreq.JobResult) {
	fmt.Printf("Job Results completed in %s for %s/%s\n",
		printDuration(result.Duration), result.Job.Bucket, result.Job.Key)
	if result.Status == wordfreq.JobCompleteFailure {
		fmt.Println("Failed:", result.StatusMessage)
		return
	}

	fmt.Println("Top Words:")
	for _, w := range result.Words {
		format := "- %s\t%d\n"
		if len(w.Word) <= 5 {
			format = "- %s\t\t%d\n"
		}
		fmt.Printf(format, w.Word, w.Count)
	}
}

func printDuration(dur time.Duration) string {
	nano := dur.Nanoseconds()
	if dur > time.Minute {
		nano = (nano / 1e9) * 1e9
	} else if dur > time.Second {
		nano = (nano / 1e6) * 1e6
	} else if dur > time.Millisecond  {
		nano = (nano / 1e3) * 1e3
	}
	return time.Duration(nano).String()
}