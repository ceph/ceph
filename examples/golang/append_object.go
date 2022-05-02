package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/smithy-go/ptr"

	"log"
	"os"
)

func uploadObject(bucketName, objectName string, svc *s3.S3) {
	// Open an object:
	obj, err := os.Open(objectName)
	if err != nil {
		log.Println("Open file failed:", err)
	}
	defer obj.Close()

	// Upload an object:
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		Body:   obj,
	})
	if err != nil {
		log.Printf("Error uploading object to s3: %v\n", err)
	} else {
		log.Printf("Successfully uploaded object %v to bucket %v.\n", objectName, bucketName)
	}
}

// This is a utility function to help create a bucket if not existed.
func createBucket(bucketName string, svc *s3.S3) {
	// Create a bucket according to bucketName:
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Printf("Error create a bucket to s3: %v\n", err)
	}
}

// This is a utility function to help show an object is successfully uploaded.
func listObject(bucketName string, svc *s3.S3) {
	// List objects:
	output, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Printf("Error listing bucket: %v\n", err)
	}

	fmt.Printf("Listing objects in %v:\n", bucketName)
	for _, object := range output.Contents {
		log.Printf("key = %s\n", ptr.ToString(object.Key))
	}
}

func main() {
	// Set bucket name and object name:
	bucketName := "mybucket"
	objectName := "myimage.jpg"
	region := "default"
	endpoint := "http://127.0.0.1:8000"
	accessKeyID := "0555b35654ad1656d804"
	secretAccessKey := "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="

	// Create a S3 session:
	svc := s3.New(session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})))

	// Create an object:
	_, err := os.Create(objectName)
	if err != nil {
		log.Printf("Error creating object: %v\n", err)
	}

	// Create a bucket if not existed:
	createBucket(bucketName, svc)

	// Upload an object to bucket:
	uploadObject(bucketName, objectName, svc)

	// List objects in bucket:
	listObject(bucketName, svc)
}
