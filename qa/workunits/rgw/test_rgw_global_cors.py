import requests
from requests_aws4auth import AWS4Auth
import json
try:
    import xmltodict
except ModuleNotFoundError:
    print("Module 'xmltodict' is not installed.")


ACCESS_KEY = '0555b35654ad1656d804'
SECRET_KEY = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
REGION = 'us-east-1'  # Replace with your bucket's region

# 1. Initialize AWS4Auth for Signature Version 4
# This generates the signature and handles the Authorization header automatically
auth = AWS4Auth(ACCESS_KEY, SECRET_KEY, REGION, 's3')

url = 'http://localhost:8000/'
origin = "https://www.your-website.com"
custom_headers = 'Content-Type,Authorization'
methods = 'GET,PUT, DELETE'

# 1. Step: Send the Preflight OPTIONS Request
preflight_headers = {
    "Host": "8.8.8.8",
    'Origin': origin,
    'Access-Control-Request-Method': methods,
    'Access-Control-Request-Headers': custom_headers
}

# 2. Step: Send the Actual GET Request
actual_headers = {
    "Host": "8.8.8.8",
    'Origin': origin,
}

def list_buckets():
    print("Listing all buckets...")
    url = 'http://localhost:8000/'
    response = requests.get(url, headers=actual_headers, auth=auth)
    print(f"Actual Response Status: {response.status_code}")
    data_dict = xmltodict.parse(response.text, process_namespaces=False)
    json_data = json.dumps(data_dict["ListAllMyBucketsResult"]["Buckets"], indent=4)
    print(f"Buckets: {json_data}")

def create_bucket(bucket_name):
    print(f"Creating a bucket named {bucket_name}...")
    url = f'http://localhost:8000/{bucket_name}'
    response = requests.put(url, headers=actual_headers, auth=auth)
    print(f"Actual Response Status: {response.status_code}")

def delete_bucket(bucket_name):
    print(f"Deleting a bucket named {bucket_name}...")
    url = f'http://localhost:8000/{bucket_name}'
    response = requests.delete(url, headers=actual_headers, auth=auth)
    print(f"Actual Response Status: {response.status_code}")

preflight_resp = requests.options(url, headers=preflight_headers)

print(f"Preflight Status: {preflight_resp.status_code}")

# Check if the server allows the custom headers
allowed_headers = preflight_resp.headers.get('Access-Control-Allow-Headers', '')

if preflight_resp.status_code in [200, 204] and custom_headers.lower() in allowed_headers.lower():
    print(f"Preflight successful! Proceeding with {methods} request...")

    list_buckets()

    create_bucket('bkt')
    list_buckets()

    create_bucket('bkt2')
    list_buckets()

    delete_bucket('bkt')
    list_buckets()

    delete_bucket('bkt2')
    list_buckets()
