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

type ResultRecorder struct {
	tableName string
	svc       dynamodbiface.DynamoDBAPI
}

func NewResultRecorder(tableName string, svc dynamodbiface.DynamoDBAPI) *ResultRecorder {
	return &ResultRecorder{
		tableName: tableName,
		svc:       svc,
	}
}

func (r *ResultRecorder) Record(result *wordfreq.JobResult) error {
	recordItem := resultRecord{
		Filename: path.Join(result.Job.Bucket, result.Job.Key),
		Words:    map[string]int{},
	}
	for _, w := range result.Words {
		recordItem.Words[w.Word] = w.Count
	}

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

type resultRecord struct {
	Filename string // Table hash key
	Words    map[string]int
}
