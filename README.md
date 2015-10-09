# aws-go-wordfreq-sample
Word Frequency is a sample service built with AWS SDK for Go. The service highlights how can be used within a concurrent application. The service takes advantage of Amazon Simple Storage service, Amazon Simple Queue Service, Amazon DynamoDB, and AWS Elastic Beanstalk to collect and report the top 10 most common words of a text file all in the AWS Cloud.

This sample highlights how the SDK can be used to build an application, to read job messages from SQS queue when an existing or new file was uploaded to S3. A S3 bucket is configured to notify an SQS queue with information of the file uploaded. This job message will be read by one of potentially many instances of the Word Frequency service application. The service will then JSON decode the message extracting the object's bucket and key of the file uploaded. Once parsed and added to a job channel, a worker goroutine within a pool of workers will read the job from the channel, stream the object's content from S3, and count  the words. When complete the worker will send the results to the results channel so that they can be recorded to DynamoDB, and also sent to an SQS result queue for further processing.

This package is made up of a set of executable commands.

### uploads3
CLI application to upload a file from your local system to S3. Taking advantage of the S3 Upload Manager's concurrent multipart uploads.

Command line usage:
```shell
./uploads3 my-bucket my-filename
```

An additional environment variable can be set instructing the uploads3 command to wait for the file to be processed, and print out the results to the console when they are available.

* WORKER_RESULT_QUEUE_URL - The SQS queue URL where the job results will be written to. 

### worker
Service application which will read job messages from a SQS, count the top 10 words, record the results to DynamoDB, and send the results also to an additional SQS queue for further processing.

Requires the following environment variables to be set.

* WORKER_QUEUE_URL - The SQS queue URL where the service will read job messages from. Job messages are created when S3 notifies the SQS queue that a file has been uploaded to a particular bucket.
* WORKER_RESULT_QUEUE_URL - The SQS queue URL where the job results will be sent to. 
* WORKER_RESULT_TABLENAME - The name of the DynamoDB table result items should be recorded to.

Optionally the follow environment variables can be provided.

* AWS_REGION - The AWS region the worker will use for signing and making all requests to. This parameter is only optional if the service is running within an EC2 instance. If not running in an EC2 instance AWS_REGION is required.
* WORKER_MESSAGE_VISIBILITY - The ammount of time messges will be hidden in the SQS job message queue from other services when a service reads that message. Will also be used to extend the visibility timeout for long running jobs. Defaults to 60s.
* WORKER_COUNT - The number of workers in the worker pool. Defaults to the number of virtual CPUs in the system.


### createTable
CLI application to show how the SDK can be used to create a DynamoDB table, which the worker will use to record job results to.

Command line usage:
```shell
./createTable my-tablename
```


