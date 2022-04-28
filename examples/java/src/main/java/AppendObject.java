import com.amazonaws.AmazonServiceException;
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

    public static void uploadObject(String object_name, String bucket_name, String object_path, AmazonS3 s3) {
        try {
            s3.putObject(bucket_name, object_name, new File(object_path));
            System.out.printf("Successfully uploaded object %s to bucket %s.\n", object_name, bucket_name);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    // This is a utility function to help create a bucket if not existed.
    public static void createBucket(String bucket_name, AmazonS3 s3) {
        Bucket named_bucket = null;
        List<Bucket> buckets = s3.listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(bucket_name)) {
                named_bucket = b;
            }
        }

        Bucket b = null;
        if (s3.doesBucketExistV2(bucket_name)) {
            System.out.format("Bucket %s already exists.\n", bucket_name);
            b = named_bucket;
        } else {
            try {
                b = s3.createBucket(bucket_name);
            } catch (AmazonS3Exception e) {
                System.err.println(e.getErrorMessage());
            }
        }
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

        // Create a S3 client:
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().
                withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region)).
                build();

        // Create a bucket if not exists:
        createBucket(bucket_name, s3);

        // Create an object:
        try{
            File file = new File(object_name);
            file.createNewFile();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }

        // Upload an object:
        uploadObject(object_name, bucket_name, object_path, s3);

        // List objects:
        System.out.printf("Listing objects in %s.\n", bucket_name);
        listObjects(bucket_name, s3);
    }
}
