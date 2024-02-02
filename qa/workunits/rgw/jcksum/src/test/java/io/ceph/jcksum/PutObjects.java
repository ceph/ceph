/**
 * 
 */
package io.ceph.jcksum;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.*;
import java.nio.*; // ByteBuffer
import java.nio.file.Files.*; //newByteChannel
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.channels.*;
import java.lang.Math.*;

import io.ceph.jcksum.*;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.*;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.*; // AttributeMap
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.core.sync.*; // RequestBody
import software.amazon.awssdk.core.checksums.*;
import software.amazon.awssdk.core.checksums.Algorithm;

import org.junit.jupiter.api.*; /* BeforeAll, Test, &c */
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance.*;

import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

/**
 * 
 */
@TestInstance(Lifecycle.PER_CLASS)
class PutObjects {

	public AwsCredentials creds;
	public URI http_uri;
	static S3Client client;
	
	void generateFile(String in_file_path, String out_file_path, long length) {
		try {
			Path ifp = Paths.get(in_file_path);
			File f = ifp.toFile();

			long if_size = f.length();
			if (if_size < (1024 * 1024)) {
				throw new IOException("in_file_path is supposed to be file-1m (i.e., a 1Mb file");
			}

			File of = new File(out_file_path);
			if (of.exists()) {
				of.delete();
			}
			
			FileOutputStream fout = new FileOutputStream(of);
			FileChannel wch = fout.getChannel();

			long resid = length;
			long r_offset = 0;
			long f_resid = 0;
			
			FileInputStream fin = new FileInputStream(f);
			FileChannel rch = fin.getChannel();
			
			while (resid > 0) {
				long to_write = Long.min(resid, f_resid);
				while (to_write > 0) {
					long written = rch.transferTo(r_offset, to_write, wch);
					r_offset += written;
					to_write -= written;
					resid -= written;
					f_resid -= written;
				}
				if (f_resid < 0) {
					throw new IOException("read overrun (logic error)");
				}
				if (f_resid == 0) {
					rch.position(0);
					f_resid = 1024 * 1024;
					r_offset = 0;
					
				}
			}
			if (rch != null) {
				rch.close();
			}
			if (wch != null) {
				wch.close();
			}
		} catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
	} /* generateFile */


  String get_envvar(String key, String defstr) {
    String var = System.getenv(key);
    if (var == null) {
      return defstr;
    }
    return var;
  }

  void readEnvironmentVars() {
    jcksum.access_key = get_envvar("AWS_ACCESS_KEY_ID", "0555b35654ad1656d804");
    jcksum.secret_key = get_envvar("AWS_SECRET_ACCESS_KEY", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==");
    jcksum.http_endpoint = get_envvar("RGW_HTTP_ENDPOINT_URL", "");
  } /* readEnvironmentVArs */

	void generateBigFiles() {
		generateFile("file-1m", "file-5m", 5 * 1024 * 1024);
		generateFile("file-1m", "file-10m", 10 * 1024 * 1024);
		generateFile("file-1m", "file-100m", 100 * 1024 * 1024);
		/* the next lengths happen to be prime */
		generateFile("file-1m", "file-5519b", 5519);
		generateFile("file-1m", "file-204329b", 204329);
		generateFile("file-1m", "file-1038757b", 1038757);
	}

	@BeforeAll
	void setup() throws URISyntaxException {

    readEnvironmentVars();

    System.out.println("PutObjects.java: starting test run:");
    System.out.println("\tAccessKey=" + jcksum.access_key);
    System.out.println("\tSecretKey=" + jcksum.secret_key);
    System.out.println("\tEndpointUrl=" + jcksum.http_endpoint);

		creds = AwsBasicCredentials.create(jcksum.access_key, jcksum.secret_key);
		http_uri = new URI(jcksum.http_endpoint);

		SdkHttpClient apacheHttpClient = ApacheHttpClient.builder()
	            .buildWithDefaults(AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true).build());
		
		/* https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html */
        client = S3Client.builder()
          .endpointOverride(http_uri)
          .credentialsProvider(StaticCredentialsProvider.create(creds))
          .region(jcksum.region)
          .forcePathStyle(true)
          .build();

    generateBigFiles();

    /* create test bucket if it doesn't exist yet */
		try {
      jcksum.createBucket(client, jcksum.bucket_name);
		} catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
  } /* setup */

	/* TODO: zap */
	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#inputFileNames")
	void testWithExplicitLocalMethodSource(String argument) {
	    assertNotNull(argument);
	    System.out.println("arg: " + argument);
	}

	boolean compareFileDigests(String lhp, String rhp) throws IOException {
		String lh5 = jcksum.getSHA512Sum(lhp);
		String rh5 = jcksum.getSHA512Sum(rhp);
		return lh5.equals(rh5);
	}
	
	boolean putAndVerifyCksum(S3Client s3, String in_file_path) {
		boolean md5_check = false;
		try {
			String out_key_name = "out_key_name"; // name we'll give the object in S3
			PutObjectResponse put_rsp = jcksum.putObjectFromFile(s3, in_file_path, out_key_name);
			String out_file_path = "out_file_name"; // name of the temp object when we download it back
			GetObjectResponse get_rsp = jcksum.GetObject(s3, out_key_name, out_file_path);
			md5_check = compareFileDigests(in_file_path, out_file_path);
		} catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
		return md5_check;
	}

	boolean putAndVerifyNoCksum(S3Client s3, String in_file_path) {
		boolean md5_check = false;
		try {
			String out_key_name = "out_key_name"; // name we'll give the object in S3
			PutObjectResponse put_rsp = jcksum.putObjectFromFileNoCksum(s3, in_file_path, out_key_name);
			String out_file_path = "out_file_name"; // name of the temp object when we download it back
			GetObjectResponse get_rsp = jcksum.GetObject(s3, out_key_name, out_file_path);
			md5_check = compareFileDigests(in_file_path, out_file_path);
		} catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
		return md5_check;
	}

	boolean mpuAndVerifyCksum(S3Client s3, String in_file_path) {
		boolean md5_check = false;
		try {
			String out_key_name = "out_key_name"; // name we'll give the object in S3
			CompleteMultipartUploadResponse put_rsp = jcksum.mpuObjectFromFile(s3, in_file_path, out_key_name);
			String out_file_path = "out_file_name"; // name of the temp object when we download it back
			GetObjectResponse get_rsp = jcksum.GetObject(s3, out_key_name, out_file_path);
			md5_check = compareFileDigests(in_file_path, out_file_path);
		} catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
		return md5_check;
	}

	boolean mpuAndVerifyNoCksum(S3Client s3, String in_file_path) {
		boolean md5_check = false;
		try {
			String out_key_name = "out_key_name"; // name we'll give the object in S3
			CompleteMultipartUploadResponse put_rsp = jcksum.mpuObjectFromFileNoCksum(s3, in_file_path, out_key_name);
			String out_file_path = "out_file_name"; // name of the temp object when we download it back
			GetObjectResponse get_rsp = jcksum.GetObject(s3, out_key_name, out_file_path);
			md5_check = compareFileDigests(in_file_path, out_file_path);
		} catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
		}
		return md5_check;
	}
	
	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#inputFileNames")
	void putObjectFromFileCksum(String in_file_path) {
		boolean rslt = false;
		System.out.println("putObjectFromFileCksum called with " + in_file_path);
		rslt = putAndVerifyCksum(client, in_file_path);
		assertTrue(rslt);
	}
	
	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#inputFileNames")
	void putObjectFromFileNoCksum(String in_file_path) {
		boolean rslt = false;
		System.out.println("putObjectFromFileNoCksum called with " + in_file_path);
		rslt = putAndVerifyNoCksum(client, in_file_path);
		assertTrue(rslt);
	}

	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#mpuFileNames")
	void mpuObjectFromFileCksum(String in_file_path) {
		boolean rslt = false;
		System.out.println("mpuObjectFromFileCksum called with " + in_file_path);
		rslt = mpuAndVerifyCksum(client, in_file_path);
		assertTrue(rslt);
	}

	@ParameterizedTest
	@MethodSource("io.ceph.jcksum.jcksum#mpuFileNames")
	void mpuObjectFromFileNoCksum(String in_file_path) {
		boolean rslt = false;
		System.out.println("mpuObjectFromFileNoCksum called with " + in_file_path);
		rslt = mpuAndVerifyNoCksum(client, in_file_path);
		assertTrue(rslt);
	}

} /* class PutObjects */
