# S3 Policy Fuzzing Suite

A comprehensive fuzzing suite for testing S3 bucket policy implementations, specifically designed to test Ceph RadosGW (RGW) resilience against malformed and malicious S3 policies.

## 📋 Overview

This suite consists of two main components:

1. **`fuzz_s3_policy.py`** - Policy generator that creates thousands of fuzzed S3 policies
2. **`s3_proxy_fuzz.py`** - Transparent HTTP proxy that serves fuzzed policies to RGW

## 🏗️ Architecture

```
Client Request → s3_proxy_fuzz.py → RGW
                      ↓
              (Injects fuzzed policy body
               for ?policy requests)
                      ↓
              Loads from pre-generated
              policy files
```

## 🚀 Quick Start

### 1. Generate Fuzzed Policies

```bash
# Generate basic fuzzed policies (safer)
python3 fuzz_s3_policy.py --policies 5

# Generate aggressive fuzzed policies (may crash RGW)
python3 fuzz_s3_policy.py --aggressive --policies 10
```

### 2. Start the Fuzzing Proxy

```bash
# Use basic policies
python3 s3_proxy_fuzz.py --port 8080 --rgw-endpoint http://localhost:8000

# Use aggressive policies
python3 s3_proxy_fuzz.py --aggressive --port 8080 --rgw-endpoint http://localhost:8000
```

## 📊 Tool 1: `fuzz_s3_policy.py`

### Purpose
Generates thousands of fuzzed S3 bucket policies using various attack vectors and malicious payloads.

### Features

- **Base Policy Generation**: Creates valid S3 policies with random values
- **Payload Injection**: Applies hundreds of malicious payloads to every field
- **Two Fuzzing Modes**: Basic (safer) and Aggressive (destructive)
- **Caching System**: Avoids regenerating identical policy sets
- **File Output**: Saves all policies as individual JSON files

### Usage

#### Basic Commands

```bash
# Generate 5 base policies with basic payloads
python3 fuzz_s3_policy.py

# Generate 10 base policies
python3 fuzz_s3_policy.py --policies 10

# Enable aggressive mode 
python3 fuzz_s3_policy.py --aggressive --policies 5
```

#### Advanced Options

```bash
# Cache management
python3 fuzz_s3_policy.py --list-cache          # Show cached policy sets
python3 fuzz_s3_policy.py --cache-stats         # Show cache statistics
python3 fuzz_s3_policy.py --clear-cache         # Clear all cached policies
python3 fuzz_s3_policy.py --force-regenerate    # Ignore cache, regenerate

# Output control
python3 fuzz_s3_policy.py --append-results      # Don't clear existing results
python3 fuzz_s3_policy.py --no-cache           # Disable caching entirely
```

### Output Directories

- **Basic Mode**: `s3_fuzz_results/` - Contains policies with basic payloads
- **Aggressive Mode**: `s3_fuzz_results_aggressive/` - Contains policies with destructive payloads

### Payload Categories

#### Basic Mode Payloads (~25 payloads)
- Empty strings and null values
- Basic path traversal (`../../../etc/passwd`)
- AWS ARN wildcards (`arn:aws:s3:::*`)
- JSON injection attempts
- Large strings (buffer overflow attempts)
- Boolean/number confusion

#### Aggressive Mode Payloads (~400 payloads)
- **Command Injection**: `; cat /etc/passwd`, `$(uname -a)`
- **SQL Injection**: `' OR '1'='1`, `'; DROP TABLE users; --`
- **Buffer Overflows**: Strings of 100KB+ size
- **Memory Corruption**: Null bytes, format strings (`%s%s%s`)
- **XML/XXE Attacks**: External entity injection
- **Protocol Confusion**: `file://`, `data:`, `gopher://`
- **Unicode Attacks**: BOM, zero-width characters
- **RGW-Specific**: Ceph pool names, RadosGW admin endpoints
- **SSRF Attempts**: Internal metadata endpoints
- **Regex DoS**: Patterns causing catastrophic backtracking

### Example Output

```json
{
  "Version": "FUZZ_PAYLOAD",
  "Id": "S3PolicyId-4283",
  "Statement": [
    {
      "Sid": "../../../../etc/passwd",
      "Effect": "Allow",
      "Principal": {"AWS": "'; DROP TABLE users; --"},
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::test-bucket-1234/*"
    }
  ]
}
```

---

## 🕸️ Tool 2: `s3_proxy_fuzz.py`

### Purpose
Transparent HTTP proxy that intercepts S3 policy requests and injects pre-generated fuzzed policies while forwarding all other requests unchanged.

### Features

- **Transparent Proxying**: Forwards all non-policy requests unchanged
- **Policy Injection**: Replaces policy request bodies with fuzzed policies
- **File-Based Loading**: Uses pre-generated policies from disk
- **Bucket Customization**: Adapts policies to requested bucket names
- **Statistics Tracking**: Monitors request patterns and policy usage
- **Error Handling**: Graceful fallbacks for missing policies

### Usage

#### Basic Commands

```bash
# Start with basic policies
python3 s3_proxy_fuzz.py --port 8080 --rgw-endpoint http://localhost:8000

# Start with aggressive policies
python3 s3_proxy_fuzz.py --aggressive --port 8080 --rgw-endpoint http://localhost:8000

# Check policy status
python3 s3_proxy_fuzz.py --status
```

#### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--port` | Proxy port | 8080 |
| `--rgw-endpoint` | RGW server URL | http://localhost:8000 |
| `--host` | Host to bind to | 0.0.0.0 |
| `--aggressive` | Use aggressive policies | False |
| `--status` | Check policy directories | - |

### Behavior

#### Policy Requests (`?policy`)
- Intercepts requests containing `?policy`
- Loads random fuzzed policy from appropriate directory
- Replaces request body with fuzzed policy JSON
- Forwards to RGW with fuzzed body
- **Headers**: Preserved exactly as received
- **Response**: Returned exactly as received from RGW

#### Non-Policy Requests
- Forwards transparently to RGW
- No modifications to headers, body, or response
- Acts as a transparent HTTP proxy

### Example Request Flow

```
1. Client: GET /mybucket?policy
2. Proxy: Loads random fuzzed policy
3. Proxy: POST /mybucket?policy (with fuzzed JSON body)
4. RGW: Processes fuzzed policy
5. RGW: Returns response (success/error)
6. Proxy: Forwards exact response to client
```

---

## 🔧 Setup & Requirements

### Dependencies

```bash
# Install required packages
pip install aiohttp

# Or if using requirements file
pip install -r requirements.txt
```

### Directory Structure

```
restler/utils/
├── fuzz_s3_policy.py          # Policy generator
├── s3_proxy_fuzz.py           # Fuzzing proxy
├── jsonfuzzer/                # Fuzzing library
│   ├── fuzzer.py
│   ├── path_finder.py
│   └── util.py
├── s3_fuzz_results/           # Basic policies (auto-created)
├── s3_fuzz_results_aggressive/ # Aggressive policies (auto-created)
└── policy_cache/              # Cache directory (auto-created)
```
