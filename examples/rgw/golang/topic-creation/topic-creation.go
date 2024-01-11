package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

func main() {
	topic := flag.String("t", "", "The name of the topic")
	attributes := flag.String("a", "", "Topic attributes needed")
	flag.Parse()

	attributesmap := map[string]*string{}
	err := json.Unmarshal([]byte(*attributes), &attributesmap) // convert JSON string to Go map
	if err != nil {
		exitErrorf("Check your JSON String for any errors: %s : %s", err, *attributes)
	}

	if *topic == "" {
		fmt.Println("You must supply the name of the topic")
		fmt.Println("-t TOPIC")
		return
	}

	if *attributes == "" {
		fmt.Println("You must supply topic attributes")
		fmt.Println("-a ATTRIBUTES")
		return
	}
	//Ceph RGW Cluster credentials
	access_key := "0555b35654ad1656d804"
	secret_key := "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
	token_id := ""
	url := "http://127.0.0.1:8000"

	defaultResolver := endpoints.DefaultResolver()
	snsCustResolverFn := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == "sns" {
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
			EndpointResolver: endpoints.ResolverFunc(snsCustResolverFn),
		},
	}))

	client := sns.New(sess)

	results, err := client.CreateTopic(&sns.CreateTopicInput{
		Attributes: attributesmap,
		Name:       topic,
	})

	if err != nil {
		exitErrorf("Unable to create topic %s, %s", *topic, err)
	}

	fmt.Printf("Succesfully created %s \n", *results.TopicArn)
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
