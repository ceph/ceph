package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	s3session *s3.S3
	//Creating a new S3 session
)

const (
	//name of previously created bucket. This bucket was created using the command
	//MON=1 OSD=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n -d  for starting the cluster then
	//s3cmd --no-ssl --host=localhost:8000 --host-bucket="localhost:8000/%(bucket)" \
	//--access_key=0555b35654ad1656d804 \
	//--secret_key=h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== \
	//mb s3://bucketname
	BUCKET_NAME = "cephbucket"
	REGION      = "us-west-2"
)

func init() {
	s3session = s3.New(session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(REGION),
		Endpoint:         aws.String("http://localhost:8000"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})))
}

func listBuckets() (resp *s3.ListBucketsOutput) {
	resp, err := s3session.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		panic(err)
	}

	return resp
}

func uploadObject(filename string) (resp *s3.PutObjectOutput) {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	fmt.Println("Uploading:", filename)
	resp, err = s3session.PutObject(&s3.PutObjectInput{
		Body:   f,
		Bucket: aws.String(cephbucket),
		Key:    aws.String(filename),
	})

	if err != nil {
		panic(err)
	}

	return resp
}

func listObjects() (resp *s3.ListObjectsV2Output) {
	resp, err := s3session.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(BUCKET_NAME),
	})

	if err != nil {
		panic(err)
	}

	return resp
}

func main() {
	fmt.Println("Hello World")
	uploadObject("golang.png")
	fmt.Println(listObjects())
}
