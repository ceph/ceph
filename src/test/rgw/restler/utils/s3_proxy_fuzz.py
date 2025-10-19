#!/usr/bin/env python3
"""
S3 Proxy Fuzzer - Uses pre-generated fuzzed policies from fuzz_s3_policy.py
Loads policies from s3_fuzz_results/ or s3_fuzz_results_aggressive/ directories
"""

import asyncio
import aiohttp
import json
import argparse
import os
import sys
import hmac
import hashlib
import logging
from urllib.parse import urlparse, unquote

# Import helper functions from fuzz_s3_policy module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'endpoint_handlers', 'policy_handler'))
from fuzz_s3_policy import (
    load_policies_from_directory,
    get_random_policy_from_list,
    print_policy_directories_status,
    check_policy_directories_status
)

try:
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import ReadOnlyCredentials, CredentialResolver, Credentials
    import boto3
except Exception:
    raise ImportError("boto3 and botocore are required. Please install them with: pip3 install boto3 botocore")

# Logging will be configured in main() based on --log-path argument

class S3ProxyFuzzer:
    def __init__(self, rgw_endpoint, host='0.0.0.0', port=8080, aggressive_mode=False, settings_path=None, enable_policy_fuzzing=False):
        self.rgw_endpoint = rgw_endpoint.rstrip('/')
        self.host = host
        self.port = port
        self.session = None
        self.aggressive_mode = aggressive_mode
        self.enable_policy_fuzzing = enable_policy_fuzzing
        
        # Determine policy directory based on mode
        self.policy_dir = "s3_fuzz_results_aggressive" if aggressive_mode else "s3_fuzz_results"
        
        # Load pre-generated policies only if policy fuzzing is enabled
        if self.enable_policy_fuzzing:
            if not os.path.exists(self.policy_dir):
                msg = f"❌ Policy directory not found: {self.policy_dir}"
                print(msg)
                logging.error(msg)
                print("Run fuzz_s3_policy.py first to generate policies:")
                if self.aggressive_mode:
                    print("  python3 endpoint_handlers/policy_handler/fuzz_s3_policy.py --aggressive --policies 5")
                else:
                    print("  python3 endpoint_handlers/policy_handler/fuzz_s3_policy.py --policies 5")
                self.policies = []
            else:
                # Use helper function from fuzz_s3_policy module
                self.policies = load_policies_from_directory(self.policy_dir)
                
                if self.policies:
                    msg = f"🎯 Fuzzing mode: {'AGGRESSIVE' if self.aggressive_mode else 'NORMAL'}"
                    print(msg)
                    logging.info(msg)
        else:
            self.policies = []
            logging.info("Policy fuzzing is DISABLED")
        
        # Load credentials and settings from settings file
        self.settings_path = settings_path
        self.aws_settings = self.load_aws_settings()
        
        # Statistics
        self.policy_requests_served = 0
        self.total_requests = 0
        
    def load_aws_settings(self):
        """Load AWS credentials and settings from JSON settings file"""
        if not self.settings_path:
            error_msg = "Settings file path is required. Use --settings to specify the full path to the JSON config file."
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        try:
            with open(self.settings_path, 'r') as f:
                settings = json.load(f)
            
            # Extract auth data from nested structure
            auth_data = settings.get('authentication', {}).get('signing', {}).get('module', {}).get('data', {})

            if not auth_data:
                logging.error(f"No authentication data found in settings file: {self.settings_path}")
                raise ValueError("Invalid settings structure")

            logging.info(f"Loaded AWS settings from: {self.settings_path}")
            logging.info(f"  Access Key: {auth_data.get('access_key', 'NOT FOUND')}")
            logging.info(f"  Region: {auth_data.get('region', 'NOT FOUND')}")
            logging.info(f"  Service: {auth_data.get('service', 'NOT FOUND')}")
            logging.info(f"  Host: {auth_data.get('host', 'NOT FOUND')}")
            
            return auth_data
            
        except FileNotFoundError:
            logging.error(f"Config file not found: {self.settings_path}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in settings file: {self.settings_path} - {e}")
            raise
        except Exception as e:
            logging.error(f"Error loading settings: {e}")
            raise
    
    def normalize_policy_path(self, path):
        """
        Normalize policy request paths to remove redundant query parameters.
        
        The OpenAPI spec defines paths like '/{Bucket}?policy' with a query parameter 
        'policy=true', which results in paths like '/bucket?policy?policy=true'.
        This function normalizes such paths to '/bucket?policy'.
        
        Args:
            path: Original path that may contain redundant query parameters
            
        Returns:
            Normalized path with single ?policy query parameter
        """
        # Check if this is a policy request with redundant parameters
        if '?policy' in path.lower():
            # Split on first ?
            parts = path.split('?', 1)
            bucket_path = parts[0]  # /bucket
            
            # Simply return bucket path with ?policy
            # This handles cases like:
            # /bucket?policy?policy=true -> /bucket?policy
            # /bucket?policy -> /bucket?policy (unchanged)
            return f"{bucket_path}?policy"
        
        return path
    
    async def start_session(self):
        """Start aiohttp session for outgoing requests"""
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=10, connect=2, sock_read=8)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            trust_env=False
        )
    
    async def handle_client(self, reader, writer):
        """Handle client connection with fuzzing integration"""
        client_addr = writer.get_extra_info('peername')
        self.total_requests += 1
        
        try:
            # Read the request line
            request_line = await reader.readline()
            if not request_line:
                return

            request_line = request_line.decode('utf-8', errors='ignore').strip()
            logging.info(f"Received request line: {request_line}")

            # Parse method, path, version
            try:
                parts = request_line.split(' ', 2)
                method = parts[0].upper()
                full_path = parts[1] if len(parts) > 1 else '/'
                http_version = parts[2] if len(parts) > 2 else 'HTTP/1.1'
            except Exception as e:
                logging.error(f"Failed to parse request line: {request_line} ({e})")
                method, full_path, http_version = 'GET', '/', 'HTTP/1.1'

            # Read headers
            headers = {}
            content_length = 0

            while True:
                line = await reader.readline()
                if not line or line == b'\r\n' or line == b'\n':
                    break
                line = line.decode('utf-8', errors='ignore').strip()
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()

                    key_lower = key.lower()
                    if key_lower not in [k.lower() for k in headers.keys()]:
                        headers[key] = value

                        if key_lower == 'content-length':
                            try:
                                content_length = int(value)
                            except Exception as e:
                                logging.error(f"Failed to parse content-length: {value} ({e})")
                                content_length = 0

            # Read body
            body = b''
            if content_length > 0:
                try:
                    body = await reader.read(content_length)
                except Exception as e:
                    logging.error(f"Failed to read body: {e}")
                    body = b''

            logging.info(f"Request: {method} {full_path} Headers: {headers} Body length: {len(body)}")

            # Check if this is a policy request
            if '?policy' in full_path.lower():
                logging.info(f"Handling policy request: {method} {full_path}")
                # Only fuzz if policy fuzzing is enabled
                if self.enable_policy_fuzzing:
                    await self.handle_policy_request(writer, method, full_path, headers, body)
                else:
                    # Forward policy request without fuzzing
                    logging.info(f"Policy fuzzing disabled, forwarding policy request without modification")
                    await self.forward_to_rgw(writer, method, full_path, headers, body)
                return

            # Forward non-policy requests to RGW unchanged
            logging.info(f"Forwarding non-policy request: {method} {full_path}")
            await self.forward_to_rgw(writer, method, full_path, headers, body)

        except Exception as e:
            # Send error response
            error_response = f"HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
            error_response += json.dumps({"error": f"Proxy error: {str(e)}"})
            logging.error(f"Proxy error: {str(e)}")

            try:
                writer.write(error_response.encode('utf-8'))
                await writer.drain()
            except Exception as e2:
                logging.error(f"Failed to send error response: {e2}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                logging.error(f"Failed to close writer: {e}")
    
    async def handle_policy_request(self, writer, method, full_path, headers, body):
        """Handle policy requests with fuzzed policies - forward to RGW with fuzzed body"""
        self.policy_requests_served += 1
        
        try:
            # Extract bucket name from path
            path_part = full_path.split('?')[0].strip('/')
            bucket_name = path_part if path_part else 'default-bucket'

            # Get a fuzzed policy directly from helper function
            try:
                policy = get_random_policy_from_list(self.policies, bucket_name=bucket_name)
            except Exception:
                # Fallback minimal policy
                policy = {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "s3:ListBucket",
                        "Resource": [f"arn:aws:s3:::{bucket_name or 'fallback-bucket'}"]
                    }]
                }

            # Convert policy to JSON for the body
            try:
                policy_json = json.dumps(policy, separators=(',', ':'), default=str)
                fuzzed_body = policy_json.encode('utf-8')
            except Exception as e:
                logging.error(f"Error serializing policy: {e}")
                # Fallback policy
                fuzzed_body = b'{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::fallback"}]}'

            # Forward to RGW with the fuzzed policy as body
            logging.info(f"Forwarding policy request for bucket '{bucket_name}' with fuzzed body ({len(fuzzed_body)} bytes)")
            await self.forward_to_rgw(writer, method, full_path, headers, fuzzed_body)

        except Exception as e:
            # Send error response
            error_response = f"HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
            error_response += json.dumps({"error": f"Policy generation error: {str(e)}"})
            logging.error(f"Policy generation error: {str(e)}")

            writer.write(error_response.encode('utf-8'))
            await writer.drain()
    
    async def forward_to_rgw(self, writer, method, path, headers, body):
        """Forward requests to RGW transparently"""
        try:
            # Normalize policy paths to remove redundant query parameters
            normalized_path = self.normalize_policy_path(path)
            
            if normalized_path != path:
                logging.info(f"Normalized path: {path} -> {normalized_path}")
            
            # Build target URL
            target_url = f"{self.rgw_endpoint}{normalized_path}"

            # Prepare headers for outgoing request (minimal filtering)
            clean_headers = {}
            skip_headers = {'host', 'content-length', 'connection', 'transfer-encoding'}

            for key, value in headers.items():
                if key.lower() not in skip_headers:
                    clean_headers[key] = value

            # Always re-sign the request for the RGW host so signatures match
            parsed_rgw = urlparse(self.rgw_endpoint)
            rgw_host = parsed_rgw.netloc
            
            try:
                signed = self._resign_headers(method, normalized_path, clean_headers, body, rgw_host)
                # Replace with signed headers
                clean_headers = signed
            except Exception as e:
                logging.error(f"Re-signing failed: {e}")
                clean_headers['Host'] = rgw_host            
            # Make request to RGW
            logging.info(f"Forwarding to RGW: {method} {target_url} Headers: {clean_headers} Body length: {len(body)}")
            async with self.session.request(
                method=method,
                url=target_url,
                headers=clean_headers,
                data=body,
                ssl=False
            ) as response:

                # Send response line exactly as received
                response_line = f"HTTP/1.1 {response.status} {response.reason}\r\n"
                writer.write(response_line.encode('utf-8'))

                # Send all response headers exactly as received (except connection-related)
                for key, value in response.headers.items():
                    if key.lower() not in {'transfer-encoding', 'connection'}:
                        header_line = f"{key}: {value}\r\n"
                        writer.write(header_line.encode('utf-8'))

                # Close connection header
                writer.write(b"Connection: close\r\n\r\n")

                # Send response body exactly as received
                response_body = await response.read()
                if response_body:
                    writer.write(response_body)

                await writer.drain()

                # Only log policy requests or errors
                if '?policy' in normalized_path.lower():
                    logging.info(f"Policy request forwarded: {method} {normalized_path} -> RGW ({response.status})")
                elif response.status >= 400:
                    logging.warning(f"Error forwarded: {method} {normalized_path} -> RGW ({response.status})")

        except Exception as e:
            # Send error response
            error_response = f"HTTP/1.1 502 Bad Gateway\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
            error_response += json.dumps({"error": f"RGW error: {str(e)}"})
            logging.error(f"RGW error: {str(e)}")

            writer.write(error_response.encode('utf-8'))
            await writer.drain()

    def _resign_headers(self, method, path, headers, body, rgw_host):
        """Re-sign the request using botocore's SigV4 so the signature matches `rgw_host`.

        Uses the same signing logic as aws_sigv4_auth.py sign_request function.
        Logs canonical request, full URL, headers, and signing details.
        """
        # If botocore/boto3 are not available, import error will be raised at startup

        # Get credentials and settings from loaded settings
        ACCESS_KEY = self.aws_settings.get('access_key')
        SECRET_KEY = self.aws_settings.get('secret_key')
        service = self.aws_settings.get('service', 's3')
        region = self.aws_settings.get('region', 'default')

        if not ACCESS_KEY or not SECRET_KEY:
            raise RuntimeError('AWS credentials not found in settings')

        # Create credentials
        credentials = Credentials(
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY
        )

        # Log what request will be signed (original request details)
        logging.info(f"========== RE-SIGNING REQUEST ==========")
        logging.info(f"Preparing to sign request:")
        logging.info(f"  Method: {method}")
        logging.info(f"  Path: {path}")
        logging.info(f"  Full URL to be signed: {self.rgw_endpoint.rstrip('/') + path}")
        logging.info(f"  Original headers: {headers}")
        logging.info(f"  Body length: {len(body) if body else 0}")

        # Prepare headers to sign
        headers_to_sign = {}
        headers_to_sign["Host"] = rgw_host

        # Include content hash if not already present
        if 'X-Amz-Content-SHA256' not in headers_to_sign:
            content_hash = hashlib.sha256(body if isinstance(body, bytes) else (body.encode('utf-8') if body else b'')).hexdigest()
            headers_to_sign['X-Amz-Content-SHA256'] = content_hash

        # Build full URL for signing
        endpoint = self.rgw_endpoint.rstrip('/')
        request_path = path
        full_url = f"{endpoint}{request_path}"

        # Log the exact parameters that will be passed to boto for signing
        logging.info(f"Parameters to be signed by boto:")
        logging.info(f"  method: {method}")
        logging.info(f"  url: {full_url}")
        logging.info(f"  headers: {headers_to_sign}")
        logging.info(f"  body length: {len(body) if body else 0}")
        if body:
            logging.info(f"body: {(body if isinstance(body, bytes) else body.encode('utf-8'))}")

        # Create request with parsed URL components to maintain encoding
        request = AWSRequest(
            method=method,
            url=full_url,
            data=body,
            headers=headers_to_sign
        )

        # Log signing details before signing
        logging.info(f"Signing with AWS SigV4:")
        logging.info(f"  Service: {service}")
        logging.info(f"  Region: {region}")
        logging.info(f"  Access Key: {ACCESS_KEY}")

        # Sign the request
        signer = SigV4Auth(credentials, service, region)
        signer.add_auth(request)

        # Log canonical request and signed headers
        try:
            canonical_request = getattr(request, 'canonical_request', None)
            if canonical_request:
                logging.info(f"Canonical request:\n{canonical_request}")
        except Exception as e:
            logging.warning(f"Could not log canonical request: {e}")

        signed_headers = dict(request.headers)
        logging.info(f"Signed headers: {signed_headers}")
        return signed_headers
    
    def print_statistics(self):
        """Print proxy statistics"""
        print(f"\n📊 Proxy Statistics:")
        print(f"  Total requests: {self.total_requests}")
        print(f"  Policy requests: {self.policy_requests_served}")
        print(f"  Forwarded requests: {self.total_requests - self.policy_requests_served}")
        print(f"  Available policies: {len(self.policies)}")
        print(f"  Policy hit rate: {(self.policy_requests_served / max(1, self.total_requests)) * 100:.1f}%")
    
    async def start_server(self):
        """Start the S3 proxy fuzzer server"""
        if self.enable_policy_fuzzing and not self.policies:
            print("❌ Policy fuzzing is enabled but no policies loaded. Cannot start server.")
            print("Generate policies first using fuzz_s3_policy.py")
            return
        
        await self.start_session()
        
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port,
            reuse_address=True,
            reuse_port=True,
            backlog=100
        )
        
        addr = server.sockets[0].getsockname()
        print(f"\n🚀 S3 Proxy Fuzzer running on {addr[0]}:{addr[1]}")
        print(f"📡 Forwarding to: {self.rgw_endpoint}")
        
        if self.enable_policy_fuzzing:
            print(f"🎯 Policy Fuzzing: ENABLED")
            print(f"📂 Policy source: {self.policy_dir} ({len(self.policies)} policies)")
            print(f"🎯 Mode: {'AGGRESSIVE' if self.aggressive_mode else 'NORMAL'}")
            
            if self.aggressive_mode:
                print(f"⚠️  WARNING: Aggressive mode - may cause RGW instability!")
        else:
            print(f"🎯 Policy Fuzzing: DISABLED (use --enable-policy-fuzzing to enable)")
        
        print(f"\nProxy will:")
        print(f"  • Forward all requests to RGW transparently")
        if self.enable_policy_fuzzing:
            print(f"  • Replace policy request bodies with fuzzed policies")
        else:
            print(f"  • Forward policy requests without modification")
        print(f"  • Re-sign all requests for RGW host")
        print(f"\n🧪 Test: curl http://localhost:{self.port}/bucket?policy\n")
        
        try:
            await server.serve_forever()
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            self.print_statistics()
        finally:
            server.close()
            await server.wait_closed()
            if self.session:
                await self.session.close()


# Note: directory status check is provided by imported helper


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='S3 Proxy Fuzzer - Uses pre-generated fuzzed policies')
    parser.add_argument('--port', type=int, default=8080, help='Proxy port (default: 8080)')
    parser.add_argument('--rgw-endpoint', type=str, required=True, help='RGW endpoint (e.g., http://localhost:8000)')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--aggressive', action='store_true', 
                       help='Use aggressive mode policies from s3_fuzz_results_aggressive/')
    parser.add_argument('--status', action='store_true',
                       help='Check policy directory status and exit')
    parser.add_argument('--settings', type=str, required=True,
                       help='FULL path to JSON settings file containing AWS credentials and settings')
    parser.add_argument('--log-path', type=str, required=False,
                       help='FULL path to log file. If not provided, logging is disabled.')
    parser.add_argument('--enable-policy-fuzzing', action='store_true',
                       help='Enable policy fuzzing. If not set, proxy forwards requests without modification.')
    
    args = parser.parse_args()
    
    # Configure logging if log path is provided
    if args.log_path:
        if not os.path.isabs(args.log_path):
            print(f"❌ Log path must be absolute: {args.log_path}")
            return
        
        # Ensure log directory exists
        log_dir = os.path.dirname(args.log_path)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception as e:
                print(f"❌ Failed to create log directory: {e}")
                return
        
        logging.basicConfig(
            filename=args.log_path,
            filemode='a',
            format='%(asctime)s %(levelname)s %(message)s',
            level=logging.INFO
        )
        print(f"📝 Logging to: {args.log_path}")
    else:
        # Disable logging to file, only console output
        logging.basicConfig(
            level=logging.WARNING,
            format='%(asctime)s %(levelname)s %(message)s'
        )
        print(f"⚠️  Logging disabled (use --log-path to enable)")
    
    if args.status:
        check_policy_directories_status()
        return
    
    # Validate settings file is provided
    if not args.settings:
        print(f"❌ Settings file is required!")
        print(f"Usage: python3 s3_proxy_fuzz.py --settings /full/path/to/engine_settings.json")
        return
    
    if not os.path.isabs(args.settings):
        print(f"❌ Settings path must be absolute: {args.settings}")
        return
    
    if not os.path.exists(args.settings):
        print(f"❌ Settings file not found: {args.settings}")
        return
    
    print("🎯 S3 Proxy Fuzzer - File-based Policy Loading")
    print("=" * 50)
    print(f"📄 Using settings file: {args.settings}")
    
    # Check policy availability only if fuzzing is enabled
    if args.enable_policy_fuzzing:
        policy_dir = "s3_fuzz_results_aggressive" if args.aggressive else "s3_fuzz_results"
        if not os.path.exists(policy_dir):
            print(f"❌ Policy directory not found: {policy_dir}")
            print("\n📝 To generate policies, run:")
            if args.aggressive:
                print("  python3 fuzz_s3_policy.py --aggressive --policies 5")
            else:
                print("  python3 fuzz_s3_policy.py --policies 5")
            print("\n📊 To check status: python3 s3_proxy_fuzz.py --status")
            return

    proxy = S3ProxyFuzzer(
        args.rgw_endpoint, 
        args.host, 
        args.port, 
        aggressive_mode=args.aggressive,
        settings_path=args.settings,
        enable_policy_fuzzing=args.enable_policy_fuzzing
    )
    
    try:
        asyncio.run(proxy.start_server())
    except KeyboardInterrupt:
        print("Server stopped")


if __name__ == '__main__':
    main()
