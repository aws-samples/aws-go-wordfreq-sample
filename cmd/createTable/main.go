package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Creates a table for the Word Frequency worker to write results to. Takes a
// single parameter for the table name to create.
//
// Usage:
//  createTable <table name>
func main() {
	if len(os.Args) != 2 {
		fmt.Printf("usage: %s <tablename>\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}
	tableName := os.Args[1]

	svc := dynamodb.New(nil)
	if _, err := svc.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Filename"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Filename"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}); err != nil {
		fmt.Println("failed to create Amazon DynamoDB table,", err)
		os.Exit(1)
	}

	fmt.Println("succesffully created", tableName)
}
