#!/usr/bin/env bash

# Build and Run uploader
AWS_REGION="us-west-2" \
AWS_PROFILE="go-wordfreq" \
WORKER_RESULT_QUEUE_URL="https://sqs.us-west-2.amazonaws.com/762127142917/ResultsQueue" \
go run ./cmd/uploads3/main.go go-wordfreq ./assets/apache2.0lic.txt

# Build and run worker in the shell
go build -o bin/application ./cmd/worker
AWS_REGION="us-west-2" \
AWS_PROFILE="default" \
WORKER_QUEUE_URL="https://sqs.us-west-2.amazonaws.com/762127142917/UploadedQueue" \
WORKER_RESULT_QUEUE_URL="https://sqs.us-west-2.amazonaws.com/762127142917/ResultsQueue" \
WORKER_RESULT_TABLENAME="wordfreq_results" \
./bin/application

# Build and run worker in the Docker
docker build -t go-wordfreq .
docker run -it \
-e "AWS_REGION=us-west-2" \
-e "WORKER_QUEUE_URL=https://sqs.us-west-2.amazonaws.com/762127142917/UploadedQueue" \
-e "WORKER_RESULT_QUEUE_URL=https://sqs.us-west-2.amazonaws.com/762127142917/ResultsQueue" \
-e "WORKER_RESULT_TABLENAME=wordfreq_results" \
--name go-wordfreq-dev \
--rm go-wordfreq

# build and archive worker app for beanstalk go environment.
env GOOS=linux GOARCH=amd64 go build -o bin/application ./cmd/worker
zip -r -X ../go-wordfreq-bin.zip . -i bin/application
