
package main

import (
  "testing"
  "fmt"
  "context"
  "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

/* tests */

func TestPut1(t *testing.T) {
  err := putObject(ctx, s3Client, bucketName)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }
}

func TestPutCksums(t *testing.T) {
  var err error
  err = putObjectCksum(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc32)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = putObjectCksum(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc32c)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = putObjectCksum(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmSha1)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = putObjectCksum(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmSha256)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = putObjectCksum(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc64nvme)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }
}

func TestChunkedUploadCrc32(t *testing.T) {
  var err error
  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc32)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc32c)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmSha1)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmSha256)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = demonstrateChunkedUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc64nvme)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }
}

func TestFixedLengthUploadCrc32(t *testing.T) {
  var err error
  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc32)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc32c)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmSha1)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmSha256)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }

  err = demonstrateFixedLengthUpload2(ctx, s3Client, bucketName,
    types.ChecksumAlgorithmCrc64nvme)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }
}

func TestMain(m *testing.M) {
  fmt.Println("gcksum golang testing")
  fmt.Println()
  fmt.Println()

  ctx  = context.Background()
  s3Client = setupS3Client(ctx)

  wg.Add(1)
  setupBucket(ctx, s3Client, bucketName)
  wg.Wait()

  // call flag.Parse() here if TestMain uses flags
	m.Run()
}
