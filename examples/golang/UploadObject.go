package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Credentials struct {
	AccessKey       string
	SecretAccessKey string
}

func main() {
	var bucket, key string
	var timeout time.Duration
	var creds Credentials

	flag.StringVar(&creds.AccessKey, "ak", "", "Access Key.")
	flag.StringVar(&creds.SecretAccessKey, "sk", "", "Secret Access Key.")
	flag.StringVar(&bucket, "b", "", "Bucket name.")
	flag.StringVar(&key, "k", "", "Object key.")
	flag.DurationVar(&timeout, "d", 0, "Upload timeout.")
	flag.Parse()

	// All clients require a Session. The Session provides the client with
	// configuration of region, endpoint, and credentials. A
	// Session is for given accesskey and secret key

	bucketlink := "http://localhost:8000/" + bucket //This is the RGW endpoint for the user
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(creds.AccessKey, creds.SecretAccessKey, ""),
		Region:      aws.String("us-west-1"),
		Endpoint:    aws.String(bucketlink),
	}))

	// Create a new instance of the service's client with a Session.

	svc := s3.New(sess)

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	// Ensure the context is canceled to prevent leaking.

	if cancelFn != nil {
		defer cancelFn()
	}

	// Uploads the object to the bucket. The Context will interrupt the request if the
	// timeout expires.
	_, err := svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   os.Stdin,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "failed to upload object, %v\n", err)
		}
		os.Exit(1)
	}

	fmt.Printf("successfully uploaded file to %s/%s\n", bucket, key)

	//Lists objects in the bucket after uploading

	objects := []string{}
	e := svc.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	},
		func(p *s3.ListObjectsOutput, lastPage bool) bool {
			for _, o := range p.Contents {
				objects = append(objects, aws.StringValue(o.Key))
			}
			return true // continue paging
		})
	if err != nil {
		panic(fmt.Sprintf("failed to list objects for bucket, %s, %v", bucket, e))
	}

	fmt.Println("Objects in bucket:", objects)
}

