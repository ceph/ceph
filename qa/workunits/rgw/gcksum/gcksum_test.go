
package main

import (
  "testing"
  "fmt"
  "context"
)

/* tests */

func TestPut1(t *testing.T) {
  err := putObject(ctx, s3Client, bucketName)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }
}

func TestChunkedUpload(t *testing.T) {
  err := demonstrateChunkedUpload(ctx, s3Client, bucketName)
  if (err != nil) {
    t.Errorf("Failed: %v", err)
  }
}

func TestFixedLengthUpload(t *testing.T) {
  err := demonstrateFixedLengthUpload(ctx, s3Client, bucketName)
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
