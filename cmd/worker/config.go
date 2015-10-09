package main

import (
	"fmt"
	"os"
	"strconv"
	"runtime"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
)

const defaultMessageVisibilityTimeout = 60
var defaultWorkerCount = runtime.NumCPU();

// A Config provides a collection of configuration values the service will use
// to setup its components.
type Config struct {
	// SQS queue URL job messages will be available at
	WorkerQueueURL           string
	// SQS queue URL job results will be written to
	ResultQueueURL           string
	// DynamoDB tablename results will be recorded to
	ResultTableName          string
	// Number of workers in the worker pool
	NumWorkers               int
	// The amount of time in seconds a read job message from the SQS will be
	// hidden from other readers of the queue.
	MessageVisibilityTimeout int64
}

// getConfig collects the configuration from the environment variables, and
// returns it, or error if it was unable to collect the configuration.
func getConfig() (Config, error) {
	c := Config{
		WorkerQueueURL:  os.Getenv("WORKER_QUEUE_URL"),
		ResultQueueURL:  os.Getenv("WORKER_RESULT_QUEUE_URL"),
		ResultTableName: os.Getenv("WORKER_RESULT_TABLENAME"),
	}

	if c.WorkerQueueURL == "" {
		return c, fmt.Errorf("missing WORKER_QUEUE_URL")
	}
	if c.ResultQueueURL == "" {
		return c, fmt.Errorf("missing WORKER_RESULT_QUEUE_URL")
	}
	if c.ResultTableName == "" {
		return c, fmt.Errorf("missing WORKER_RESULT_TABLENAME")
	}

	if os.Getenv("AWS_REGION") == "" {
		region, err := ec2metadata.New(nil).Region()
		if err != nil {
			return c, fmt.Errorf("region not specified, unable to retrieve from EC2 instance %v", err)
		}
		defaults.DefaultConfig.Region = aws.String(region)
	}

	if timeoutStr := os.Getenv("WORKER_MESSAGE_VISIBILITY"); timeoutStr != "" {
		timeout, err := strconv.ParseInt(timeoutStr, 10, 64)
		if err != nil {
			return c, err
		}
		if timeout <= 0 {
			return c, fmt.Errorf("invalid message visibility timeout")
		}
		c.MessageVisibilityTimeout = timeout
	} else {
		c.MessageVisibilityTimeout = defaultMessageVisibilityTimeout
	}

	atOnceStr := os.Getenv("WORKER_COUNT")
	if atOnceStr == "" {
		c.NumWorkers = defaultWorkerCount
	} else {
		atOnce, err := strconv.ParseInt(atOnceStr, 10, 64)
		if err != nil {
			return c, err
		}
		if atOnce <= 0 {
			return c, fmt.Errorf("invalid worker number")
		}
		c.NumWorkers = int(atOnce)
	}

	return c, nil
}
