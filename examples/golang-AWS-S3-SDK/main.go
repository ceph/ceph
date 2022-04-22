package main

import (
  "fmt"
  "strings"
  "os"
  "io/ioutil"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/s3"
  "github.com/aws/aws-sdk-go/aws/awserr"
)

var (
  s3session *s3.S3
)

const (
  BUCKET_NAME = "sahi123"
  REGION = "eu-central-1"
)

func init() {
  s3session = s3.New(session.Must(session.NewSession(&aws.Config{
    Region: aws.String(REGION),
    })))
}

func listBuckets() (resp *s3.ListBucketsOutput) {
  resp, err := s3session.ListBuckets(&s3.ListBucketsInput{})
  if err != nil {
    panic(err)
  }

  return resp
}

func createBucket() (resp *s3.CreateBucketOutput) {
  resp, err := s3session.CreateBucket(&s3.CreateBucketInput{
    // ACL: aws.String(s3.BucketCannedACLPrivate),
    // ACL: aws.String(s3.BucketCannedACLPublicRead),
    Bucket: aws.String(BUCKET_NAME),
    CreateBucketConfiguration: &s3.CreateBucketConfiguration{
      LocationConstraint: aws.String(REGION),
    },
  })
  if err != nil {
    if aerr, ok := err.(awserr.Error); ok {
      switch aerr.Code() {
      case s3.ErrCodeBucketAlreadyExists:
        fmt.Println("Bucket name already in use!")
        panic(err)
      case s3.ErrCodeBucketAlreadyOwnedByYou:
        fmt.Println("Bucket exists and is owned by you!")
      default:
        panic(err)
      }
    }
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
    Body: f,
    Bucket: aws.String(BUCKET_NAME),
    Key: aws.String(strings.Split(filename, "/")[1]),
    ACL: aws.String(s3.BucketCannedACLPublicRead),
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

func getObject(filename string) () {
  fmt.Println("Downloading: ", filename)

  resp, err := s3session.GetObject(&s3.GetObjectInput{
    Bucket: aws.String(BUCKET_NAME),
    Key: aws.String(filename),
  })

  if err != nil {
    panic(err)
  }

  body, err := ioutil.ReadAll(resp.Body)
  err = ioutil.WriteFile(filename, body, 0644)
  if err != nil {
    panic(err)
  }
}

func deleteObject(filename string) (resp *s3.DeleteObjectOutput) {
  fmt.Println("Deleting: ", filename)
  resp, err := s3session.DeleteObject(&s3.DeleteObjectInput{
    Bucket: aws.String(BUCKET_NAME),
    Key: aws.String(filename),
  })

  if err != nil {
    panic(err)
  }

  return resp
}

func main() {
  folder := "files"

  files, _ := ioutil.ReadDir(folder)
  fmt.Println(files)
  for _, file := range files {
    if file.IsDir() {
      continue
    } else {
      uploadObject(folder + "/" + file.Name())
    }
  }

  fmt.Println(listObjects())

  for _, object := range listObjects().Contents {
    getObject(*object.Key)
    deleteObject(*object.Key)
  }

  fmt.Println(listObjects())
}
