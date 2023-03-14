# Quiche HTTP/3 Frontend

## Building

Use the cmake option `WITH_RADOSGW_QUICHE` to enable the frontend. This will clone and build the frontend's dependencies:

* Quiche, a rust library for QUIC and HTTP/3
* BoringSSL, an OpenSSL fork required by Quiche
* CURL, which we build and link against BoringSSL/Quiche for testing

The only additional build dependency is the `cargo` command required to build Quiche.

## Testing

### Create a self-signed test certificate

	~/ceph/build $ openssl req -new -newkey rsa:4096 -x509 -sha256 -days 365 -nodes -out rgw.crt -keyout rgw.key

### Start a test cluster with both frontends

	~/ceph/build $ OSD=1 MON=1 RGW=1 MGR=0 MDS=0 ../src/vstart.sh -n -d --rgw_frontend 'beast ssl_port=8000 ssl_certificate=rgw.crt ssl_private_key=rgw.key, quiche port=8000 ssl_certificate=rgw.crt ssl_private_key=rgw.key'

### Using CURL for HTTP/3

The Quiche-enabled curl command is placed under the bin/ subdirectory with the rest of ceph's binaries.

To issue HTTP/3 requests, add the `--http3` option and provide a url starting with `https://`.

To sign requests as the vstart `testid` user:

	--aws-sigv4 aws:amz:us-east-1:s3 -u '0555b35654ad1656d804:h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

Because the ssl certificate is self-signed, you either need to disable curl's verification with the insecure option `-k`, or provide the environment variable `SSL_CERT_FILE=rgw.crt`.

#### Create a testbucket

	~/ceph/build $ bin/curl {options} https://localhost:8000/testbucket -X PUT -v

#### Create, upload, and download an object

	dd if=/dev/zero of=128m.iso bs=1M count=128
	~/ceph/build $ bin/curl {options} https://localhost:8000/testbucket/128m.iso -T 128m.iso -v
	~/ceph/build $ bin/curl {options} https://localhost:8000/testbucket/128m.iso --output 128m.iso -v

## Wireshark

Wireshark can be used to inspect QUIC/HTTP3 traffic, but extra configuration is necessary to view packets encrypted with TLS.

First, set the `SSLKEYLOGFILE` environment variable to enable the logging of keys on the client and server:

	export SSLKEYLOGFILE=/path/to/sslkeys.log

Then in Wireshark, make the following changes in the `Edit -> Preferences -> Protocols` menu:

* QUIC: uncheck `Reassemble out-of-order CRYPTO frames`
* TLS: set `(Pre)-Master-Secret log filename` to `/path/to/sslkeys.log`
