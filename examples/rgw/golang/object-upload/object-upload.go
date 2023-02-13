package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	bucket := flag.String("b", "", "The name of the bucket")
	filename := flag.String("f", "", "Complete file path to object to be uploaded")
	flag.Parse()

	if *bucket == "" {
		fmt.Println("You must supply the name of the bucket")
		fmt.Println("-b BUCKET")
		return
	}

	if *filename == "" {
		fmt.Println("You must supply the object to be uploaded")
		fmt.Println("-f FILE/FILEPATH")
		return
	}

	file, err := os.Open(*filename)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", filename, err)
	}

	defer file.Close()

	//Ceph RGW Cluster credentials
	access_key := "0555b35654ad1656d804"
	secret_key := "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
	token_id := ""
	url := "http://127.0.0.1:8000"

	defaultResolver := endpoints.DefaultResolver()
	s3CustResolverFn := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == "s3" {
			return endpoints.ResolvedEndpoint{
				URL: url,
			}, nil
		}

		return defaultResolver.EndpointFor(service, region, optFns...)
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:           aws.String("default"),
			Credentials:      credentials.NewStaticCredentials(access_key, secret_key, token_id),
			S3ForcePathStyle: aws.Bool(true),
			EndpointResolver: endpoints.ResolverFunc(s3CustResolverFn),
		},
	}))

	uploader := s3manager.NewUploader(sess)

	// Upload the file's body to S3 bucket as an object with the key being the
	// same as the filename.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: bucket,
		Key:    filename,
		Body:   file,
	})
	if err != nil {
		exitErrorf("Unable to upload %q to %q, %v", *filename, *bucket, err)
	}

	fmt.Printf("Successfully uploaded %q to %q\n", *filename, *bucket)
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
