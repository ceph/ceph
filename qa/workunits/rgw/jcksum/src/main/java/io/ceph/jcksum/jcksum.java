package io.ceph.jcksum;

import java.io.*;
import java.util.*;
import java.net.*; // HTTP, URI, ...
import java.util.stream.*;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.*;
import software.amazon.awssdk.utils.*; // AttributeMap
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.core.sync.*; // RequestBody
import software.amazon.awssdk.core.checksums.*;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.core.waiters.*;

/* MD5Sum */
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.codec.digest.DigestUtils;

public class jcksum {

	static Region region = Region.US_EAST_1;
	static S3Client client, ssl_client;
	
	static String bucket_name = "sheik";
	static String object_name = "jerbuti";
	static String access_key = "0555b35654ad1656d804";
	static String secret_key = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==";
	
	static String http_endpoint = "http://192.168.111.1:8000";
	static String ssl_endpoint = "https://192.168.111.1:8443";
	
	static int mpu_size = 5 * 1024 * 1024;

	/* files containing test data of the corresponding names/sizes */
	public static Stream<String> inputFileNames() {
	    return Stream.of(
	    		"file-8b",
	    		"file-200b",
	    		"file-21983b",
	    		"file-5519b",
	    		"file-204329b",
	    		"file-256k",
	    		"file-1m",
	    		"file-1038757b"
	    		);
	} /* inputFileNames */

	public static Stream<String> mpuFileNames() {
	    return Stream.of(
	    		"file-5m",
	    		"file-10m",
	    		"file-100m"
	    		);
	} /* mpuFileNames */

  public static void createBucket(S3Client s3Client, String bucket_name) {
    try {
      S3Waiter s3Waiter = s3Client.waiter();
      CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
        .bucket(bucket_name)
        .build();

      s3Client.createBucket(bucketRequest);
      HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
        .bucket(bucket_name)
        .build();

      // Wait until the bucket is created and print out the response.
      WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
      waiterResponse.matched().response().ifPresent(System.out::println);
      System.out.println(bucket_name +" is ready");

    } catch (S3Exception e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  } /* createBucket */

	public static void listBucket(S3Client s3) {
        try {
            ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(bucket_name)
                .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (S3Object obj: objects) {
            	System.out.println(
            		String.format("obj key: %s owner: %s size: %d", obj.key(), obj.owner(), obj.size()));
            }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
	}

	public static GetObjectResponse GetObject(S3Client s3, String in_key_name, String out_file_name) {
			GetObjectResponse resp = null;
			
			GetObjectRequest get_req =
					GetObjectRequest.builder()
						.bucket(bucket_name)
						.key(in_key_name)
						.build();
			try {
				File f = new File(out_file_name);
				if (f.exists()) {
					f.delete();
				}
				resp = s3.getObject(get_req, ResponseTransformer.toFile(f));
			}  catch (S3Exception e) {
	            System.err.println(e.awsErrorDetails().errorMessage());
	            System.exit(1);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
			
			return resp;
	}
	
	public static CompleteMultipartUploadResponse mpuObjectFromFile(S3Client s3, String in_file_path, String out_key_name) {
		File f = new File(in_file_path);
		CompleteMultipartUploadResponse completedUploadResponse = null;
		CreateMultipartUploadRequest create_req =
				CreateMultipartUploadRequest.builder()
					.bucket(bucket_name)
					.key(out_key_name)
					.checksumAlgorithm(ChecksumAlgorithm.SHA256)
					.build();
		
		CreateMultipartUploadResponse createdUpload = s3.createMultipartUpload(create_req);
		
		/* the file streaming method shown in aws-doc-sdk-examples/.../CheckObjectIntegrity.java
		 * creates a FileInputStream from a file, but then copies each chunk into a ByteBuffer by
		 * hand before uploading--which per code comments, forces RequestBody to copy the buffer
		 * again before sending it--let's see if we can use RequestBody.fromInputStream() instead,
		 * it seems to be designed for this purpose (I'm not clear why you would share the InputStream,
		 * and the only apparent reason to prefer the buffer even with an async client seems to be
		 * avoid a deferred close on it) */
		
		try {
			InputStream in = new FileInputStream(f);
			List<CompletedPart> completedParts = new ArrayList<CompletedPart>();
			int partNumber = 1;
			
			for (long resid = f.length(); resid > 0;) {
				long bytes = Math.min(mpu_size, resid);
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .partNumber(partNumber)
                        .uploadId(createdUpload.uploadId())
                        .bucket(bucket_name)
                        .key(out_key_name)
                        .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                        .build();
                UploadPartResponse uploadedPart = s3.uploadPart(uploadPartRequest,
                	RequestBody.fromInputStream(in, bytes));
                CompletedPart part = CompletedPart.builder().
                        partNumber(partNumber)
                        .checksumSHA256(uploadedPart.checksumSHA256())
                        .eTag(uploadedPart.eTag()).build();
                completedParts.add(part);
                partNumber++;
                resid -= bytes;
			} /* for all chunks * bytes */
			
            CompletedMultipartUpload completedMultipartUpload =
            		CompletedMultipartUpload.builder().parts(completedParts).build();
            completedUploadResponse = s3.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucket_name)
                            .key(out_key_name)
                            .uploadId(createdUpload.uploadId())
                            .multipartUpload(completedMultipartUpload).build());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return completedUploadResponse;
	} /* mpuObjectFromFile */
	
	public static CompleteMultipartUploadResponse mpuObjectFromFileNoCksum(S3Client s3, String in_file_path, String out_key_name) {
		File f = new File(in_file_path);
		CompleteMultipartUploadResponse completedUploadResponse = null;
		CreateMultipartUploadRequest create_req =
				CreateMultipartUploadRequest.builder()
					.bucket(bucket_name)
					.key(out_key_name)
					/* .checksumAlgorithm(ChecksumAlgorithm.SHA256) */
					.build();
		
		CreateMultipartUploadResponse createdUpload = s3.createMultipartUpload(create_req);
		
		/* the file streaming method shown in aws-doc-sdk-examples/.../CheckObjectIntegrity.java
		 * creates a FileInputStream from a file, but then copies each chunk into a ByteBuffer by
		 * hand before uploading--which per code comments, forces RequestBody to copy the buffer
		 * again before sending it--let's see if we can use RequestBody.fromInputStream() instead,
		 * it seems to be designed for this purpose (I'm not clear why you would share the InputStream,
		 * and the only apparent reason to prefer the buffer even with an async client seems to be
		 * avoid a deferred close on it) */
		
		try {
			InputStream in = new FileInputStream(f);
			List<CompletedPart> completedParts = new ArrayList<CompletedPart>();
			int partNumber = 1;
			
			for (long resid = f.length(); resid > 0;) {
				long bytes = Math.min(mpu_size, resid);
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .partNumber(partNumber)
                        .uploadId(createdUpload.uploadId())
                        .bucket(bucket_name)
                        .key(out_key_name)
                        /* .checksumAlgorithm(ChecksumAlgorithm.SHA256) */
                        .build();
                UploadPartResponse uploadedPart = s3.uploadPart(uploadPartRequest,
                	RequestBody.fromInputStream(in, bytes));
                CompletedPart part = CompletedPart.builder().
                        partNumber(partNumber)
                        .checksumSHA256(uploadedPart.checksumSHA256())
                        .eTag(uploadedPart.eTag()).build();
                completedParts.add(part);
                partNumber++;
                resid -= bytes;
			} /* for all chunks * bytes */
			
            CompletedMultipartUpload completedMultipartUpload =
            		CompletedMultipartUpload.builder().parts(completedParts).build();
            completedUploadResponse = s3.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucket_name)
                            .key(out_key_name)
                            .uploadId(createdUpload.uploadId())
                            .multipartUpload(completedMultipartUpload).build());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return completedUploadResponse;
	} /* mpuObjectFromFileNoCksum */
	
	/* without mpu and without explicit checksum request, chunked encoding is
	 * not (automatically?) sent;  with a checksum specified, it is */
	public static PutObjectResponse putObjectFromFileNoCksum(S3Client s3, String in_file_path, String out_key_name) {
       	PutObjectResponse resp = null;
		try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-wax", "ahatchee");
            PutObjectRequest putOb = PutObjectRequest.builder()
                .bucket(bucket_name)
                .key(out_key_name)
                .metadata(metadata)
                .build();

            resp = s3.putObject(putOb, RequestBody.fromFile(new File(in_file_path))); // "using the full contents of the specified file"

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return resp;
    } /* putObjectFromFileNoCksum */
	
	/* without mpu and without explicit checksum request, chunked encoding is
	 * not (automatically?) sent;  with a checksum specified, it is */
	public static PutObjectResponse putObjectFromFile(S3Client s3, String in_file_path, String out_key_name) {
		PutObjectResponse resp = null;
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-wax", "ahatchee");
            PutObjectRequest putOb = PutObjectRequest.builder()
                .bucket(bucket_name)
                .key(out_key_name)
                .metadata(metadata)
                .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                .build();

            RequestBody rbody = RequestBody.fromFile(new File(in_file_path));
            resp = s3.putObject(putOb, rbody); // "using the full contents of the specified file"
            System.out.println("PutObjectResponse");
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return resp;
    } /* putObjectFromFile */
	
	
    public static String getMD5Sum(String filePath) throws IOException {
        try (InputStream is = Files.newInputStream(Paths.get(filePath))) {
            return DigestUtils.md5Hex(is);
        }
    }
    
    public static String getSHA512Sum(String filePath) throws IOException {
        try (InputStream is = Files.newInputStream(Paths.get(filePath))) {
            return DigestUtils.sha512Hex(is);
        }
    }
	
	public static void main(String[] args) throws URISyntaxException {
		
		AwsCredentials creds = AwsBasicCredentials.create(access_key, secret_key);
		URI http_uri = new URI(http_endpoint);
		
		/* ah, yeah.  so many options.
		 * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
		 */		
		SdkHttpClient apacheHttpClient = ApacheHttpClient.builder()
	            .buildWithDefaults(AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true).build());
		
		/* https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html */
        client = S3Client.builder()
        		.endpointOverride(http_uri)
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .region(region)
                .build();

		URI ssl_uri = new URI(ssl_endpoint);
        ssl_client = S3Client.builder()
        		.httpClient(apacheHttpClient)
        		.endpointOverride(ssl_uri)
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .region(region)
                .build();
		
        //listBucket(client);
        //listBucket(ssl_client);
        
        String out_name = "object_out";
        
        // if !ssl, we see x-amz-trailer-signature (in the trailer)
        //putObjectFromFile(client, "file-8b", out_name); // minimal STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER case
        putObjectFromFile(client, "file-200b", out_name); // STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER (multi) (200) (checksum?)
        //putObjectFromFile(client, "file-21983b", out_name); // STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER (multi) (200) (checksum?)
        //putObjectFromFile(client, "file-256k", out_name); // x-amz-content-sha256:STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER (multi) (200) (checksum?)
        //putObjectFromFile(client, "file-1M", out_name); // x-amz-content-sha256:STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER (multi) (200) (checksum?)
       
        /* ok to here! */
        
        // XXXX minimal streaming unsigned checksum trailer case
        //putObjectFromFile(ssl_client, "file-8b", out_name);
        //putObjectFromFile(ssl_client, "file-200b", out_name); // STREAMING-UNSIGNED-PAYLOAD-TRAILER (400)
        //putObjectFromFile(ssl_client, "file-21983b", out_name); // x-amz-content-sha256:STREAMING-UNSIGNED-PAYLOAD-TRAILER (400)
        //putObjectFromFile(ssl_client, "file-256k", out_name); //x-amz-content-sha256:STREAMING-UNSIGNED-PAYLOAD-TRAILER (multi) (400)
        
        // minimal, traditional awssigv4 streaming hmac sha256 case (works)
        //putObjectFromFileNoCksum(client, "file-8b", out_name);
        
        //putObjectFromFileNoCksum(client, "file-200b", object_name); // STREAMING-AWS4-HMAC-SHA256-PAYLOAD (multi) 200
        //putObjectFromFileNoCksum(ssl_client, "file-200b", out_name); // UNSIGNED-PAYLOAD (no completer) 200

        //mpuObjectFromFile(client, "file-200b", out_name); // STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER(multi) 400 (no completer?)
        //mpuObjectFromFile(client, "file-256k", out_name);
        //mpuObjectFromFile(ssl_client, "file-200b", out_name); // STREAMING-UNSIGNED-PAYLOAD-TRAILER (no completer) 400
        //mpuObjectFromFile(ssl_client, "file-256k", out_name);
        
        //mpuObjectFromFileNoCksum(client, "file-200b", out_name); // AWS4-HMAC-SHA256-PAYLOAD (no completer?) 200
        //mpuObjectFromFileNoCksum(client, "file-256k", out_name);
        //mpuObjectFromFileNoCksum(ssl_client, "file-200b", out_name); //x-amz-content-sha256:UNSIGNED-PAYLOAD (no completer) 200
        //mpuObjectFromFileNoCksum(ssl_client, "file-256k", out_name);
		System.out.println("all that way...");
	} /* main */
} /* jcksum */
