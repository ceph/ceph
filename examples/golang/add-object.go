package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const( id  = "AWS_ACCESS_KEY_ID"
 key = "AWS_SECRET_ACCESS_KEY")

func main(){
	endPoint := flag.String("EndPoint", "", "region of request")
	accessId := flag.String("Access ID", "", "Access Id")
	accessKey := flag.String("Access Key", "", "Access Key")
	objectName := flag.String("Object Path", "", "Object Path")
	bucketName := flag.String("Bucket Name", "", "The name of the s3 bucket")
	flag.Parse()
	if flag.NFlag() < 4 {
		log.Printf("Usage error: EndPoint, Access ID, Access Key, and Object Path not set.")
	}
	// I want it to be a one way flow for the app user thats why I used flags for access key and Id instead of 
	// the regular environment variable. 
	err := os.Setenv(id, accessId)
	if err != nil {
		log.Fatalf(err.Error())
	}
	err = os.Setenv(key, accessKey)
	if err != nil {
		log.Fatalf(err.Error())
	}

	
	//check if object, bucket, and endpoint is not empty. 
	//A separate logic could be added to use a default bucket anme
	// check the object flag name to figure out where it exists. An example of this: `.` `./` `../`
	//find the object on the file system 
	aws.
	
	cfg, err := config.LoadDefaultConfig(context.TODO(), 
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc()))
	if err != nil {
		log.Printf("Error loading config ", err)
	}
	params := s3.PutObjectInput{
		Bucket: bucketName,
		//key: key,
		// contentType:
		//contentlength
	}

	svc := createClient(cfg, params)
	putObject(svc, &params)
	
	
}

func createClient(config aws.Config) *s3.Client{
	svc := s3.NewFromConfig(config)
	svc.e
	return svc
}

func putObject(svc *s3.Client, params *s3.PutObjectInput) error {
	_, err := svc.PutObject(params)
	if err != nil {
		log.Printf("Error uploading object to s3: %v", err)
	}
	fmt.Printf("Successfully uploaded object %v to bucket", objectName)
	return nil
}
