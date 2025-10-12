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
import random
import glob
from urllib.parse import urlparse, unquote

class S3ProxyFuzzer:
    def __init__(self, rgw_endpoint, host='0.0.0.0', port=8080, aggressive_mode=False):
        self.rgw_endpoint = rgw_endpoint.rstrip('/')
        self.host = host
        self.port = port
        self.session = None
        self.aggressive_mode = aggressive_mode
        
        # Determine policy directory based on mode
        self.policy_dir = "s3_fuzz_results_aggressive" if aggressive_mode else "s3_fuzz_results"
        
        # Load pre-generated policies
        self.policies = self.load_policies()
        
        # Statistics
        self.policy_requests_served = 0
        self.total_requests = 0
        
    def load_policies(self):
        """Load pre-generated fuzzed policies from files"""
        policies = []
        
        if not os.path.exists(self.policy_dir):
            print(f"❌ Policy directory not found: {self.policy_dir}")
            print("Run fuzz_s3_policy.py first to generate policies:")
            if self.aggressive_mode:
                print("  python3 fuzz_s3_policy.py --aggressive --policies 5")
            else:
                print("  python3 fuzz_s3_policy.py --policies 5")
            return []
        
        # Find all policy files
        policy_files = glob.glob(os.path.join(self.policy_dir, "fuzzed_policy_*.json"))
        
        if not policy_files:
            print(f"❌ No policy files found in {self.policy_dir}")
            return []
        
        print(f"📂 Loading policies from: {self.policy_dir}")
        print(f"🔍 Found {len(policy_files)} policy files")
        
        # Load policies from files
        loaded_count = 0
        for policy_file in policy_files:
            try:
                with open(policy_file, 'r') as f:
                    policy = json.load(f)
                    policies.append(policy)
                    loaded_count += 1
            except Exception as e:
                print(f"⚠️  Failed to load {policy_file}: {e}")
                continue
        
        print(f"✅ Successfully loaded {loaded_count} fuzzed policies")
        print(f"🎯 Fuzzing mode: {'AGGRESSIVE' if self.aggressive_mode else 'NORMAL'}")
        
        return policies
    
    def get_random_policy(self, bucket_name=None):
        """Get a random fuzzed policy, optionally customized for bucket"""
        if not self.policies:
            # Fallback to basic policy if no fuzzed policies available
            return {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": [f"arn:aws:s3:::{bucket_name or 'fallback-bucket'}"]
                }]
            }
        
        # Get random policy
        policy = random.choice(self.policies).copy()
        
        # Try to customize for specific bucket if provided
        if bucket_name:
            try:
                policy_str = json.dumps(policy)
                # Replace test bucket names with actual bucket name
                customized_str = policy_str.replace("test-bucket-", f"{bucket_name}-")
                policy = json.loads(customized_str)
            except:
                pass  # Use original policy if customization fails
        
        return policy
    
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
            
            # Parse method, path, version
            try:
                parts = request_line.split(' ', 2)
                method = parts[0].upper()
                full_path = parts[1] if len(parts) > 1 else '/'
                http_version = parts[2] if len(parts) > 2 else 'HTTP/1.1'
            except:
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
                            except:
                                content_length = 0
            
            # Read body
            body = b''
            if content_length > 0:
                try:
                    body = await reader.read(content_length)
                except:
                    body = b''
            
            # Check if this is a policy request
            if '?policy' in full_path.lower():
                await self.handle_policy_request(writer, method, full_path, headers, body)
                return
            
            # Forward non-policy requests to RGW unchanged
            await self.forward_to_rgw(writer, method, full_path, headers, body)
            
        except Exception as e:
            # Send error response
            error_response = f"HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
            error_response += json.dumps({"error": f"Proxy error: {str(e)}"})
            
            try:
                writer.write(error_response.encode('utf-8'))
                await writer.drain()
            except:
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
    
    async def handle_policy_request(self, writer, method, full_path, headers, body):
        """Handle policy requests with fuzzed policies - forward to RGW with fuzzed body"""
        self.policy_requests_served += 1
        
        try:
            # Extract bucket name from path
            path_part = full_path.split('?')[0].strip('/')
            bucket_name = path_part if path_part else 'default-bucket'
            
            # Get a fuzzed policy
            policy = self.get_random_policy(bucket_name)
            
            # Convert policy to JSON for the body
            try:
                policy_json = json.dumps(policy, separators=(',', ':'), default=str)
                fuzzed_body = policy_json.encode('utf-8')
            except Exception as e:
                print(f"⚠️  Error serializing policy: {e}")
                # Fallback policy
                fuzzed_body = b'{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::fallback"}]}'
            
            # Forward to RGW with the fuzzed policy as body
            await self.forward_to_rgw(writer, method, full_path, headers, fuzzed_body)
            
            print(f"🎯 Forwarded policy request for bucket '{bucket_name}' with fuzzed body ({len(fuzzed_body)} bytes)")
            
        except Exception as e:
            # Send error response
            error_response = f"HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
            error_response += json.dumps({"error": f"Policy generation error: {str(e)}"})
            
            writer.write(error_response.encode('utf-8'))
            await writer.drain()
    
    async def forward_to_rgw(self, writer, method, path, headers, body):
        """Forward requests to RGW transparently"""
        try:
            # Build target URL
            target_url = f"{self.rgw_endpoint}{path}"
            
            # Prepare headers for outgoing request (minimal filtering)
            clean_headers = {}
            skip_headers = {'host', 'content-length', 'connection', 'transfer-encoding'}
            
            for key, value in headers.items():
                if key.lower() not in skip_headers:
                    clean_headers[key] = value
            
            # Make request to RGW
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
                if '?policy' in path.lower():
                    print(f"🎯 Policy request forwarded: {method} {path} -> RGW ({response.status})")
                elif response.status >= 400:
                    print(f"⚠️  Error forwarded: {method} {path} -> RGW ({response.status})")
        
        except Exception as e:
            # Send error response
            error_response = f"HTTP/1.1 502 Bad Gateway\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
            error_response += json.dumps({"error": f"RGW error: {str(e)}"})
            
            writer.write(error_response.encode('utf-8'))
            await writer.drain()
            print(f"❌ Forward error: {method} {path} - {str(e)}")
    
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
        if not self.policies:
            print("❌ No policies loaded. Cannot start server.")
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
        print(f"� Policy source: {self.policy_dir} ({len(self.policies)} policies)")
        print(f"🎯 Mode: {'AGGRESSIVE' if self.aggressive_mode else 'NORMAL'}")
        
        if self.aggressive_mode:
            print(f"⚠️  WARNING: Aggressive mode - may cause RGW instability!")
        
        print(f"\nProxy will:")
        print(f"  • Forward all requests to RGW transparently")
        print(f"  • Replace policy request bodies with fuzzed policies")
        print(f"  • Preserve all original headers and responses")
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


def check_policy_directories():
    """Check status of policy directories"""
    normal_dir = "s3_fuzz_results"
    aggressive_dir = "s3_fuzz_results_aggressive"
    
    print("📋 Policy Directory Status:")
    
    for dir_name, mode in [(normal_dir, "NORMAL"), (aggressive_dir, "AGGRESSIVE")]:
        if os.path.exists(dir_name):
            policy_files = glob.glob(os.path.join(dir_name, "fuzzed_policy_*.json"))
            total_size = sum(os.path.getsize(os.path.join(dir_name, f)) 
                           for f in os.listdir(dir_name) if f.endswith('.json'))
            
            print(f"  {mode}: ✅ {len(policy_files)} policies ({total_size / 1024 / 1024:.1f} MB)")
        else:
            print(f"  {mode}: ❌ Directory not found")
            print(f"    Run: python3 fuzz_s3_policy.py {'--aggressive ' if mode == 'AGGRESSIVE' else ''}--policies 5")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='S3 Proxy Fuzzer - Uses pre-generated fuzzed policies')
    parser.add_argument('--port', type=int, default=8080, help='Proxy port (default: 8080)')
    parser.add_argument('--rgw-endpoint', default="http://localhost:8000", help='RGW endpoint')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--aggressive', action='store_true', 
                       help='Use aggressive mode policies from s3_fuzz_results_aggressive/')
    parser.add_argument('--status', action='store_true',
                       help='Check policy directory status and exit')
    
    args = parser.parse_args()
    
    if args.status:
        check_policy_directories()
        return
    
    print("🎯 S3 Proxy Fuzzer - File-based Policy Loading")
    print("=" * 50)
    
    # Check policy availability
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
        aggressive_mode=args.aggressive
    )
    
    try:
        asyncio.run(proxy.start_server())
    except KeyboardInterrupt:
        print("Server stopped")


if __name__ == '__main__':
    main()
