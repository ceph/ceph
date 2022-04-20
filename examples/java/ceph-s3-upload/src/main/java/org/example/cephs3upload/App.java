package org.example.cephs3upload;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;

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
        // Note That the AWSClient builder takes in the endpoint and the region
        // This has to be specified in this file
        final AmazonS3 s3 = AmazonS3ClientBuilder
                                .standard()
                                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:8000", "default"))
                                .build();
        try {
            s3.putObject(bucket_name, key_name, new File(file_path));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        System.out.println("Done!");
    }
}
