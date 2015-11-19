package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

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

	// Create a session which contains the default configurations for the SDK.
	// Use the session to create the service clients to make API calls to AWS.
	sess := session.New()

	// Create S3 Uploader manager to concurrently upload the file
	svc := s3manager.NewUploader(sess)

	fmt.Println("Uploading file to S3...")
	result, err := svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filepath.Base(filename)),
		Body:   file,
	})
	if err != nil {
		fmt.Println("error", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully uploaded %s to %s\n", filename, result.Location)

	if queueURL := os.Getenv("WORKER_RESULT_QUEUE_URL"); queueURL != "" {
		fmt.Println("Waiting for results...")
		waitForResult(sqs.New(sess), bucket, filepath.Base(filename), queueURL)
	}
}

// waitForResult waits for the job to be processed and the job result to be added
// to the job result SQS queue.  This will pool the SQS queue for job results until
// a job result matches the file it uploaded. When a match is found the job result
// will also be deleted from the queue, and its status written to the console.
// If the job result doesn't match the file uploaded by this client, the message
// will be ignored, so another client could received it.
func waitForResult(svc sqsiface.SQSAPI, bucket, filename, resultQueueURL string) {
	for {
		resp, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:          aws.String(resultQueueURL),
			VisibilityTimeout: aws.Int64(0),
			WaitTimeSeconds:   aws.Int64(20),
		})
		if err != nil {
			log.Println("Failed to receive message", err)
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

// printResult prints the job results to the console.
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

// printDuration formats the duration trimming less significant units based on
// the overall duration provided.  Minutes will be limit to seconds. Seconds to
// milliseconds. Milliseconds to microseconds.
func printDuration(dur time.Duration) string {
	nano := dur.Nanoseconds()
	if dur > time.Minute {
		nano = (nano / 1e9) * 1e9
	} else if dur > time.Second {
		nano = (nano / 1e6) * 1e6
	} else if dur > time.Millisecond {
		nano = (nano / 1e3) * 1e3
	}
	return time.Duration(nano).String()
}
