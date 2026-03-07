# S3 RESTler Fuzzing with RGW

This guide provides step-by-step instructions for setting up and running RESTler-based fuzzing tests against Ceph RGW S3 APIs with policy fuzzing capabilities.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
  - [1. Install RESTler (AWS S3 Auth Fork)](#1-install-restler-aws-s3-auth-fork)
  - [2. Build RESTler](#2-build-restler)
  - [3. Clone Templated JSON Fuzzer](#3-clone-templated-json-fuzzer)
  - [4. Install Python Dependencies](#4-install-python-dependencies)
- [Configuration](#configuration)
  - [RGW Endpoint Configuration](#rgw-endpoint-configuration)
  - [AWS Credentials Setup](#aws-credentials-setup)
- [Running Tests](#running-tests)
  - [Generate Fuzzed Policies](#generate-fuzzed-policies)
  - [Start S3 Proxy Fuzzer](#start-s3-proxy-fuzzer)
  - [Run RESTler Fuzzing](#run-restler-fuzzing)
- [Test Modes](#test-modes)

---

## Quick Start Summary

```bash
# 1. Clone and build RESTler
sudo apt-get install -y dotnet-runtime-8.0
git clone https://github.com/Suyashd999/restler-fuzzer.git
cd restler-fuzzer && git checkout s3auth
mkdir ~/restler_bin
python3.12 ./build-restler.py --dest_dir $(realpath ~/restler_bin)

# 2. Compile OpenAPI spec and configure settings
cd ceph/src/test/rgw/restler
~/restler_bin/restler/Restler compile --api_spec openapi.yaml

# 3. IMPORTANT: Copy engine_settings.json outside Compile/ directory
# The Compile/ directory gets overwritten on every compile
cp Compile/engine_settings.json ./engine_settings.json

# 4. Edit engine_settings.json to add AWS credentials
nano engine_settings.json
# Add the authentication section with your RGW credentials
# See "Configuration" section below for detailed structure

# 5. Clone templated-json-fuzzer
cd utils/endpoint_handlers/policy_handler/
git clone https://github.com/arbitraryrw/templated-json-fuzzer.git
touch templated-json-fuzzer/jsonfuzzer/{__init__.py,core/__init__.py,parser/__init__.py,util/__init__.py}

# 6. Install dependencies
pip3 install boto3 botocore aiohttp

# 7. Generate fuzzed policies (ONLY if using --enable-policy-fuzzing)
python3 fuzz_s3_policy.py --policies 5
cd ../../

# 8. Start proxy
# Option A: Without policy fuzzing (no policy generation needed)
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings ../../engine_settings.json \
  --log-path /tmp/proxy.log

# Option B: With policy fuzzing (requires step 7)
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings ../../engine_settings.json \
  --enable-policy-fuzzing \
  --log-path /tmp/proxy.log

# 9. Run RESTler (in another terminal)
cd ceph/src/test/rgw/restler
~/restler_bin/restler/Restler test \
  --grammar_file Compile/grammar.py \
  --dictionary_file Compile/dict.json \
  --settings engine_settings.json \
  --no_ssl
```
## Prerequisites

Before starting, ensure you have the following installed:

- **Python 3.12.8** (required for RESTler)
- **.NET 8.0 SDK** (required for building RESTler)
- **Git**
- **Ceph cluster with RGW** (RadosGW) running

### Install Prerequisites

#### Python 3.12.8
```bash
# On Ubuntu/Debian
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.12 python3.12-venv python3.12-dev

# Verify installation
python3.12 --version
```

#### .NET 8.0 SDK
```bash
# On Ubuntu/Debian
sudo apt-get update 
sudo apt-get install -y dotnet-runtime-8.0

# Verify installation
dotnet --version
```

---

## Installation

### 1. Install RESTler (AWS S3 Auth Fork)

Clone the RESTler fork that includes AWS SigV4 authentication support:

```bash
# Clone the fork with S3 auth support
git clone https://github.com/Suyashd999/restler-fuzzer.git
cd restler-fuzzer

# Switch to the s3auth branch
git checkout s3auth

# Verify you're on the correct branch
git branch --show-current
# Output should be: s3auth
```

### 2. Build RESTler

```bash
# Create directory for RESTler binaries
mkdir -p ~/restler_bin

# Build RESTler from the repo root
python3.12 ./build-restler.py --dest_dir $(realpath ~/restler_bin)

# Build output will be in ~/restler_bin/
# Expected structure:
#   ~/restler_bin/
#   ├── engine/
#   ├── restler/
#   └── ...
```

**Verify the build:**
```bash
ls ~/restler_bin/restler/Restler
# Should show the Restler executable
```

### 3. Clone Templated JSON Fuzzer

The policy fuzzer requires the templated-json-fuzzer library:

```bash
# Navigate to policy_handler directory
cd ceph/src/test/rgw/restler/utils/endpoint_handlers/policy_handler/

# Clone the templated-json-fuzzer repository
git clone https://github.com/arbitraryrw/templated-json-fuzzer.git

# Add __init__.py files to make it a Python package
touch templated-json-fuzzer/jsonfuzzer/__init__.py
touch templated-json-fuzzer/jsonfuzzer/core/__init__.py
touch templated-json-fuzzer/jsonfuzzer/parser/__init__.py
touch templated-json-fuzzer/jsonfuzzer/util/__init__.py
```

**Verify the structure:**
```bash
ls templated-json-fuzzer/jsonfuzzer/
# Should show: core/  parser/  util/  __init__.py
```

### 4. Install Python Dependencies

```bash
# Install required Python packages
pip3 install boto3 botocore aiohttp

# Verify installations
python3 -c "from botocore.auth import SigV4Auth; import boto3; import aiohttp; print('All dependencies installed successfully')"
```

---

## Configuration

### RGW Endpoint Configuration

You need to configure RESTler to point to your Ceph RGW endpoint.

#### For vstart Cluster (Default)

If using a Ceph vstart development cluster:

```bash
# Default RGW endpoint for vstart
RGW_ENDPOINT="http://localhost:8000"
```

#### For Production/Custom Cluster

If using a different RGW setup, you'll need to update the endpoint.

**Step 1: Run RESTler Compile** (generates initial settings)
```bash
# Compile the OpenAPI spec
~/restler_bin/restler/Restler compile --api_spec ceph/src/test/rgw/restler/openapi.yaml
```

> **⚠️ WARNING: Compilation Overwrites Settings**
> 
> Every time you run `Restler compile`, the entire `Compile/` directory is **deleted and recreated**, 
> which means any changes to `Compile/engine_settings.json` will be **lost**.
>
> **Solution:** Copy `engine_settings.json` outside the `Compile/` directory after first compilation:
> ```bash
> cp Compile/engine_settings.json ./engine_settings.json
> ```
> Then edit and use `./engine_settings.json` instead of `Compile/engine_settings.json`.

**Step 2: Copy and Update engine_settings.json**

The `engine_settings.json` file is initially created in the `Compile/` directory. Copy it outside first:

```bash
# Copy settings file outside Compile/ directory
cp Compile/engine_settings.json ./engine_settings.json

# Edit the copied settings file
nano engine_settings.json
```

**Following structure would be created by default:**
```json
{
  "per_resource_settings": {},
  "max_combinations": 20
}
```

**Modify the structure to:**
```json
{
  "per_resource_settings": {},
  "max_combinations": 20,  
  "authentication": {
    "signing": {
      "module": {
          "file": "/absolute/path/to/restler/aws_sigv4_auth.py",
          "function": "sign_request",
          "data": {
              "access_key": "ACCESS_KEY", 
              "secret_key": "SECRET_KEY",
              "region": "default",
              "service": "s3",
            "host": "localhost:8080" // Port 8000 for direct vstart and NO proxy
          }
        }
      }
  }
}
```

**Update the `host` field to match your RGW endpoint:**
- For vstart: `"host": "localhost:8000"`
- For custom: `"host": "192.168.1.100:7480"`

---

## Running Tests

### Generate Fuzzed Policies

**Note:** This step is **only required** if you plan to run the proxy with `--enable-policy-fuzzing` flag. If you're running the proxy without policy fuzzing (transparent mode), you can skip this step.

Generate fuzzed policy files:

```bash
cd ceph/src/test/rgw/restler/utils/endpoint_handlers/policy_handler

# Generate normal mode policies (5 base policies)
python3 fuzz_s3_policy.py --policies 5

# Generate aggressive mode policies (more malicious payloads)
python3 fuzz_s3_policy.py --aggressive --policies 5

# Check generated policies
ls ../../s3_fuzz_results/
# Should show: fuzzed_policy_0000.json, fuzzed_policy_0001.json, ...
```

**Policy Generation Options:**
```bash
# Generate more policies for extended testing
python3 fuzz_s3_policy.py --policies 10

# Force regenerate (ignore cache)
python3 fuzz_s3_policy.py --policies 5 --force-regenerate

# Append to existing policies
python3 fuzz_s3_policy.py --policies 3 --append-results

# Check cache status
python3 fuzz_s3_policy.py --cache-stats

# List cached policy sets
python3 fuzz_s3_policy.py --list-cache

# Clear cache
python3 fuzz_s3_policy.py --clear-cache
```

### Start S3 Proxy Fuzzer

The S3 proxy intercepts requests, re-signs them, and optionally fuzzes policy requests.

**Important:** Policy generation (previous step) is only needed when using `--enable-policy-fuzzing` flag.

```bash
cd ceph/src/test/rgw/restler/utils

# Basic proxy (no policy fuzzing, just forwards and re-signs requests)
# No policy generation needed for this mode
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings /path/to/restler/engine_settings.json

# With policy fuzzing enabled
# REQUIRES: Generate policies first (see "Generate Fuzzed Policies" section)
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings /path/to/restler/engine_settings.json \
  --enable-policy-fuzzing

# With logging enabled
# REQUIRES: Generate policies first (see "Generate Fuzzed Policies" section)
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings /path/to/restler/engine_settings.json \
  --enable-policy-fuzzing \
  --log-path /home/Downloads/s3_proxy_fuzz.log

# Aggressive mode (uses aggressive fuzzed policies)
# REQUIRES: Generate aggressive policies first (python3 fuzz_s3_policy.py --aggressive --policies 5)
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings /path/to/restler/engine_settings.json \
  --enable-policy-fuzzing \
  --aggressive \
  --log-path /home/suyash/Downloads/s3_proxy_fuzz.log
```

**Proxy Options:**
- `--port`: Proxy listen port (default: 8080)
- `--host`: Bind address (default: 0.0.0.0)
- `--rgw-endpoint`: **Required** - RGW endpoint URL
- `--settings`: **Required** - Full path to engine_settings.json
- `--enable-policy-fuzzing`: Enable policy fuzzing (default: disabled)
- `--aggressive`: Use aggressive mode policies
- `--log-path`: Full path to log file (optional)
- `--status`: Check policy directory status and exit

**Check proxy status:**
```bash
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings /path/to/restler/engine_settings.json \
  --status
```

**Expected output when proxy starts:**
```
🎯 S3 Proxy Fuzzer - File-based Policy Loading
==================================================
📄 Using settings file: /path/to/engine_settings.json
📝 Logging to: /home/suyash/Downloads/s3_proxy_fuzz.log

🚀 S3 Proxy Fuzzer running on 0.0.0.0:8080
📡 Forwarding to: http://localhost:8000
🎯 Policy Fuzzing: ENABLED
📂 Policy source: s3_fuzz_results (67 policies)
🎯 Mode: NORMAL

Proxy will:
  • Forward all requests to RGW transparently
  • Replace policy request bodies with fuzzed policies
  • Re-sign all requests for RGW host

🧪 Test: curl http://localhost:8080/bucket?policy
```

### Run RESTler Fuzzing


**Run RESTler Test Mode:**
```bash
cd ceph/src/test/rgw/restler

# Test mode - quick validation
# Note: Using engine_settings.json from root directory (not Compile/)
~/restler_bin/restler/Restler test \
  --grammar_file Compile/grammar.py \
  --dictionary_file Compile/dict.json \
  --settings engine_settings.json \
  --no_ssl
```

**Run RESTler Fuzz Mode:**
```bash
# Fuzz mode - full fuzzing campaign
~/restler_bin/restler/Restler fuzz \
  --grammar_file Compile/grammar.py \
  --dictionary_file Compile/dict.json \
  --settings engine_settings.json \
  --no_ssl \
  --time_budget 1
```

**Run RESTler Fuzz-Lean Mode:**
```bash
# Fuzz-lean mode - lighter fuzzing
~/restler_bin/restler/Restler fuzz-lean \
  --grammar_file Compile/grammar.py \
  --dictionary_file Compile/dict.json \
  --settings engine_settings.json \
  --no_ssl \
  --time_budget 0.5
```

---

## Test Modes

### Mode 1: Transparent Proxy (No Fuzzing)
Just forwards and re-signs requests without modifying policy bodies.

**Prerequisites:** None - no policy generation needed

```bash
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings /path/to/restler/engine_settings.json
```

**Use case:** Test signature re-signing without policy fuzzing.

### Mode 2: Policy Fuzzing (Normal)
Replaces policy request bodies with fuzzed policies from `s3_fuzz_results/`.

**Prerequisites:** Generate policies first:
```bash
cd utils/endpoint_handlers/policy_handler
python3 fuzz_s3_policy.py --policies 5
cd ../../
```

```bash
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings /path/to/engine_settings.json \
  --enable-policy-fuzzing
```

**Use case:** Standard policy fuzzing with moderate payloads.

### Mode 3: Policy Fuzzing (Aggressive)
Uses aggressive fuzzed policies designed to break RGW.

**Prerequisites:** Generate aggressive policies first:
```bash
cd utils/endpoint_handlers/policy_handler
python3 fuzz_s3_policy.py --aggressive --policies 5
cd ../../
```

```bash
python3 s3_proxy_fuzz.py \
  --rgw-endpoint http://localhost:8000 \
  --settings /path/to/engine_settings.json \
  --enable-policy-fuzzing \
  --aggressive
```

**Use case:** Stress testing RGW with extreme payloads (may cause crashes).

---
