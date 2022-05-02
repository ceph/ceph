import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class AppendObject {
    public static void uploadObject(String object_name, String bucket_name, String object_path, AmazonS3 s3) throws AmazonS3Exception {
        s3.putObject(bucket_name, object_name, new File(object_path));
        System.out.printf("Successfully uploaded object %s to bucket %s.\n", object_name, bucket_name);
    }

    // This is a utility function to help create a bucket if not existed.
    public static void createBucket(String bucket_name, AmazonS3 s3) throws AmazonS3Exception {
        Bucket b = s3.createBucket(bucket_name);
    }

    // This is a utility function to help show an object is successfully uploaded.
    public static void listObjects(String bucket_name, AmazonS3 s3) {
        ListObjectsV2Result result = s3.listObjectsV2(bucket_name);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os : objects) {
            System.out.println("* " + os.getKey());
        }
    }

    public static void main(String[] args) {
        // Set parameters:
        String bucket_name = "mybucket";
        String object_path = "myimage.jpg";
        String object_name = "myimage.jpg";
        String endpoint = "http://127.0.0.1:8000";
        String region = "default";
        String access_key_id = "0555b35654ad1656d804";
        String secret_access_key = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==";

        // Create a S3 client:
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().
                withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region)).
                withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(access_key_id, secret_access_key))).
                build();

        // Create an object:
        try{
            File file = new File(object_name);
            boolean b = file.createNewFile();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }

        // Create a bucket if not exists:
        try {
            createBucket(bucket_name, s3);
        } catch (AmazonS3Exception e) {
            System.err.println(e.getErrorMessage());
        }

        // Upload an object:
        try {
            uploadObject(object_name, bucket_name, object_path, s3);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }

        // List objects:
        System.out.printf("Listing objects in %s.\n", bucket_name);
        listObjects(bucket_name, s3);
    }
}
