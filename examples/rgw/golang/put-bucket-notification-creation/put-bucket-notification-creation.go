package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	bucket := flag.String("b", "", "Name of the bucket to add notification to")
	topic := flag.String("t", "", "The topic onto which the notification is attached to")
	flag.Parse()

	if *bucket == "" {
		fmt.Println("You must supply the name of the bucket")
		fmt.Println("-b BUCKET")
		return
	}

	if *topic == "" {
		fmt.Println("You must supply the name of the topic ARN")
		fmt.Println("-t TOPIC ARN")
		return
	}

	//Ceph RGW Credentials
	access_key := "0555b35654ad1656d804"
	secret_key := "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
	token_id := ""
	url := "http://127.0.0.1:8000"

	defaultResolver := endpoints.DefaultResolver()
	CustResolverFn := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
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
			EndpointResolver: endpoints.ResolverFunc(CustResolverFn),
		},
	}))

	svc := s3.New(sess)

	suffixRule := []*s3.FilterRule{
		{
			Name:  aws.String("suffix"),
			Value: aws.String("jpg"),
		},
	}

	input := &s3.PutBucketNotificationConfigurationInput{
		Bucket: bucket,
		NotificationConfiguration: &s3.NotificationConfiguration{
			TopicConfigurations: []*s3.TopicConfiguration{
				{
					Events: []*string{aws.String("s3:ObjectCreated:*")},
					Filter: &s3.NotificationConfigurationFilter{
						Key: &s3.KeyFilter{
							FilterRules: suffixRule,
						},
					},
					Id:       aws.String("notif1"), //Raises MalformedXML if absent
					TopicArn: topic,
				},
			},
		},
	}

	_, err := svc.PutBucketNotificationConfiguration(input)

	if err != nil {
		exitErrorf("Unable to create Put Bucket Notification because of %s", err)
	}
	fmt.Println("Put bucket notification added to  ", *topic)
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
