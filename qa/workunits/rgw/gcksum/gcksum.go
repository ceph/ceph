package main

import (
	"bytes"
	"context"
  "strings"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
  "sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
  "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var region string = "us-east-1"
var bucketName string = "caijupepe"

var ctx context.Context
var s3Client *s3.Client
var wg sync.WaitGroup

func getenv(k string, defv string) string {
  v := os.Getenv(k)
  if (v == "") {
    v = defv
  }
  return v
}

func main() {
  fmt.Println("gcksum main() (moar cowbell)")

	// Setup
	ctx := context.Background()
	s3Client := setupS3Client(ctx)

  wg.Add(1)
  setupBucket(ctx, s3Client, bucketName)
  wg.Wait()

	// Tests
  var err error
  err = putObject(ctx, s3Client, bucketName);
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }

  fmt.Println()
  fmt.Println()
  fmt.Println()

  err = putObjectCksum(ctx, s3Client, bucketName, types.ChecksumAlgorithmCrc32);
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = putObjectCksum(ctx, s3Client, bucketName, types.ChecksumAlgorithmCrc32c);
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = putObjectCksum(ctx, s3Client, bucketName, types.ChecksumAlgorithmSha1);
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = putObjectCksum(ctx, s3Client, bucketName, types.ChecksumAlgorithmSha256);
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = putObjectCksum(ctx, s3Client, bucketName, types.ChecksumAlgorithmCrc64nvme);
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName, types.ChecksumAlgorithmCrc32)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName, types.ChecksumAlgorithmCrc32c)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName, types.ChecksumAlgorithmSha1)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName, types.ChecksumAlgorithmSha256)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc64nvme)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName, types.ChecksumAlgorithmCrc32)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName, types.ChecksumAlgorithmCrc32c)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName, types.ChecksumAlgorithmSha1)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName, types.ChecksumAlgorithmSha256)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()

  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc64nvme)
  if (err != nil) {
    fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
  }
  fmt.Println()
	fmt.Println()
	fmt.Println()
}

func setupS3Client(ctx context.Context) *s3.Client {

  access_key := getenv("AWS_ACCESS_KEY_ID", "0555b35654ad1656d804")
  secret_key := getenv("AWS_SECRET_ACCESS_KEY",
    "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==")

  endpoint_url := getenv("RGW_HTTP_ENDPOINT_URL", "https://localhost:8443")
	session_token := ""

	staticProvider := credentials.NewStaticCredentialsProvider(
		access_key,
		secret_key,
		session_token,
	)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}

	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithClientLogMode(aws.LogRequestWithBody|aws.LogResponseWithBody),
		config.WithCredentialsProvider(staticProvider),
	)

	if err != nil {
		fmt.Printf("failed to load AWS config: %v\n", err)
		os.Exit(1)
	}

	s3_client := s3.NewFromConfig(
		awsConfig, func(o *s3.Options) {
			o.HTTPClient = client
			o.Region = region
			o.BaseEndpoint = aws.String(endpoint_url)
			o.UsePathStyle = true
			o.RetryMaxAttempts = 1
		})

	return s3_client
}

func setupBucket(ctx context.Context, client *s3.Client, bucketName string) {
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
		},
	})
  err = err
  defer wg.Done()
}

func putObject(ctx context.Context, client *s3.Client,
  bucketName string) error {
  // simplest possible S3 PUT operation
	obj_ix := 1
	key := fmt.Sprintf("object%d", obj_ix)
	body := fmt.Sprintf("body for %s/%s", bucketName, key)
	poinput := &s3.PutObjectInput{
		Body:   strings.NewReader(body),
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err := client.PutObject(context.TODO(), poinput)
  return err
}

func putObjectCksum(ctx context.Context, client *s3.Client,
  bucketName string, ckAlgo types.ChecksumAlgorithm) error {

  key := fmt.Sprintf("obj_for_%s", ckAlgo)
  body := fmt.Sprintf("body for %s/%s", bucketName, key)
	poinput := &s3.PutObjectInput{
		Body:   strings.NewReader(body),
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
    ChecksumAlgorithm: ckAlgo,
	}
	_, err := client.PutObject(context.TODO(), poinput)
  return err
}

func demonstrateChunkedUpload2(ctx context.Context, s3Client *s3.Client,
  bucketName string, ckAlgo types.ChecksumAlgorithm) error {
	// Create an IO pipe. The total amount of data read isn't known to the
	// reader (S3 PutObject), so the PutObject call will use a chunked upload.
	pipeReader, pipeWriter := io.Pipe()

	dataToUpload := []byte("This is some example chunked data to upload to S3.")
	key := "chunked-upload-example"

	// Start a goroutine to write data to the pipe
	go func() {
		pipeWriter.Write(dataToUpload)
		pipeWriter.Close()
	}()

	// Upload the data from the pipe to S3 using a chunked upload
	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   pipeReader,
    ChecksumAlgorithm: ckAlgo,
	})

	fmt.Printf("Uploaded chunked data to S3 bucket %s with key %s\n", bucketName, key)
	fmt.Printf("Error: %v\n", err)
  return err
}

func demonstrateFixedLengthUpload2(ctx context.Context, s3Client *s3.Client,
  bucketName string, ckAlgo types.ChecksumAlgorithm) error {
	// Create a fixed-length byte slice to upload
	dataToUpload := []byte("This is some example fixed-length data to upload to S3.")
	key := "fixed-length-upload-example"

	// Using a reader-seeker ensures that the data will be uploaded as fixed length, with the
	// content length set to the size of the byte slice.
	var readerSeeker io.ReadSeeker = bytes.NewReader(dataToUpload)

	// Upload the data directly to S3
	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   readerSeeker,
    ChecksumAlgorithm: ckAlgo,
	})

	fmt.Printf("Uploaded fixed-length data to S3 bucket %s with key %s\n", bucketName, key)
	fmt.Printf("Error: %v\n", err)
  return err
}
