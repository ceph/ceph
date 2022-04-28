package main

import (
    "bufio"
    "bytes"
    "context"
    "flag"
    "fmt"
    "log"
    "os"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {

    endPoint := flag.String("endpoint", "", "EndPoint of request")
    accessId := flag.String("access_key", "", "Access Id")
    accessKey := flag.String("secret_key", "", "Access Key")
    objectPath := flag.String("objectpath", "", "Object Path")
    bucketName := flag.String("bucketName", "", "The name of the s3 bucket")
    flag.Parse()
    if flag.NFlag() < 4 {
        log.Fatalf("Usage error: EndPoint, Access ID, Access Key, and Object Path not set.")
    }

    f, err := os.Open(*objectPath)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()

    fileReader := bufio.NewReader(f)

    fileBytes, err := fileReader.ReadBytes('\n')
    if err != nil {
        log.Fatal(err)
    }

    cfg, err := config.LoadDefaultConfig(context.TODO(),
        config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*accessId, *accessKey, "")),
        config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
            aws.EndpointResolverWithOptionsFunc(
                func(service, region string, options ...interface{}) (aws.Endpoint, error) {
                    return aws.Endpoint{
                        URL:               *endPoint,
                        HostnameImmutable: true,
                    }, nil
                }))))

    if err != nil {
        log.Fatal("Error loading config ", err)
    }

    svc := createClient(cfg)
    fmt.Println(*bucketName)

    params := &s3.PutObjectInput{
        Bucket: aws.String(*bucketName),
        Key:    aws.String(*objectPath),
        Body:   bytes.NewReader(fileBytes),
    }

    putObject(svc, params, objectPath, bucketName)

}

func createClient(config aws.Config) *s3.Client {
    svc := s3.NewFromConfig(config)

    return svc
}
func putObject(svc *s3.Client, params *s3.PutObjectInput, ObjectName, bucketName *string) {
    _, err := svc.PutObject(context.TODO(), params)
    if err != nil {
        log.Fatalf("Error uploading object to s3: %v", err)
    }
    fmt.Printf("Successfully uploaded object '%v' to bucket '%v' \n", *ObjectName, *bucketName)
}