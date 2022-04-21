package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	id  = "AWS_ACCESS_KEY_ID"
	key = "AWS_SECRET_ACCESS_KEY"
)

func main() {
	endPoint := flag.String("endpoint", "", "region of request")
	accessId := flag.String("accessId", "", "Access Id")
	accessKey := flag.String("accessKey", "", "Access Key")
	ObjectName := flag.String("objectPath", "", "Object Path")
	bucketName := flag.String("bucketName", "", "The name of the s3 bucket")
	flag.Parse()
	if flag.NFlag() < 4 {
		log.Fatalf("Usage error: EndPoint, Access ID, Access Key, and Object Path not set.")
	}
	// I want it to be a one way flow for the app user thats why I used flags for access key and Id instead of
	// a direct environment variable.
	setEnv(id, *accessId)
	setEnv(key, *accessKey)

	f, err := os.Open(*ObjectName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	file := bufio.NewReader(f)

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			aws.EndpointResolverWithOptionsFunc(
				func(service, region string, options ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{
						URL: *endPoint,
					}, nil
				}))))
	if err != nil {
		log.Fatal("Error loading config ", err)
	}

	params := s3.PutObjectInput{
		Bucket: aws.String(*bucketName),
		Key:    aws.String(*ObjectName),
		Body:   file,
	}

	svc := createClient(cfg)
	putObject(svc, &params, ObjectName)

}

func setEnv(id, accessValue string) {
	err := os.Setenv(id, accessValue)
	if err != nil {
		log.Fatalf(err.Error())
	}
}

func createClient(config aws.Config) *s3.Client {
	svc := s3.NewFromConfig(config)

	return svc
}

func putObject(svc *s3.Client, params *s3.PutObjectInput, ObjectName *string) {
	_, err := svc.PutObject(context.TODO(), params)
	if err != nil {
		log.Printf("Error uploading object to s3: %v", err)
	}
	fmt.Printf("Successfully uploaded object %v to bucket", ObjectName)
}
