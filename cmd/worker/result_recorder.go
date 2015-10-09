package main

import (
	"fmt"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/awslabs/aws-go-wordfreq-sample"
)

// A ResultRecorder provides the an abstraction to record job results to DynamoDB.
type ResultRecorder struct {
	tableName string
	svc       dynamodbiface.DynamoDBAPI
}

// NewResultRecorder creates a new instance of the ResultRecorder configured
// with a DynamoDB service client.
func NewResultRecorder(tableName string, svc dynamodbiface.DynamoDBAPI) *ResultRecorder {
	return &ResultRecorder{
		tableName: tableName,
		svc:       svc,
	}
}

// Record marshals the job result into a dynamodb.AttributeValue struct, and writes
// the result item to DyanmoDB.
func (r *ResultRecorder) Record(result *wordfreq.JobResult) error {
	// Construct a result item representing what data we want to write to DynamoDB.
	recordItem := resultRecord{
		Filename: path.Join(result.Job.Bucket, result.Job.Key),
		Words:    map[string]int{},
	}
	for _, w := range result.Words {
		recordItem.Words[w.Word] = w.Count
	}

	// Use the ConvertToX helpers to marshal a Go struct to a dyanmodb.AttributeValue
	// type. This greatly simplifies the code needed to create the attribute
	// value item.
	av, err := dynamodbattribute.ConvertToMap(recordItem)
	if err != nil {
		return fmt.Errorf("unable to serialize result to dyanmoDB.AttributeValue, %v", err)
	}
	_, err = r.svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      av,
	})
	if err != nil {
		return fmt.Errorf("unable to record result, %v", err)
	}

	return nil
}

// a resultRecord represents the result item in DynamoDB.
type resultRecord struct {
	Filename string // Table hash key
	Words    map[string]int
}
