package org.example.cephs3upload;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import java.io.File;
import java.nio.file.Paths;

public class App 
{
    public static void main( String[] args )
    {
        final String USAGE = "\n" +
                "To run this example, supply the name of an S3 bucket and a file to\n" +
                "upload to it.\n" +
                "\n" +
                "Ex: java -jar target/ceph-s3-upload-1.0-SNAPSHOT-jar-with-dependencies.jar <bucketname> <filename>\n";

        if (args.length < 2) {
            System.out.println(USAGE);
            System.exit(1);
        }

        String bucket_name = args[0];
        String file_path = args[1];
        String key_name = Paths.get(file_path).getFileName().toString();

        System.out.format("Uploading %s to S3 bucket %s...\n", file_path, bucket_name);
        // Put in the CEPH RGW access and secret keys here in that order  "access key"              "secret key"
        // Must also be specified here
        BasicAWSCredentials credentials = new BasicAWSCredentials("0555b35654ad1656d804","h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==");
        // Note That the AWSClient builder takes in the endpoint and the region
        // This has to be specified in this file
        final AmazonS3 s3 = AmazonS3ClientBuilder
                                .standard()
                                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:8000", "default"))
                                .build();
        try {
            s3.putObject(bucket_name, key_name, new File(file_path));
        } catch (AmazonS3Exception e) {
            System.err.println(e.getMessage()); // raises more explicit error message than e.getErrorMessage() e.g when Bucket is not available
            System.exit(1);
        }
        System.out.println("Object upload successful!");
    }
}
