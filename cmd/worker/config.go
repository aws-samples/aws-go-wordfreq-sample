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

type Config struct {
	WorkerQueueURL           string
	ResultQueueURL           string
	ResultTableName          string
	NumWorkers               int
	MessageVisibilityTimeout int64
}

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
