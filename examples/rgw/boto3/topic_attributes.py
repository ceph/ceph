import sys
import boto3
from pprint import pprint

if len(sys.argv) == 2:
    # topic arn as first argument
    topic_arn = sys.argv[1]
else:
    print ('Usage: ' + sys.argv[0] + ' <topic arn>')
    sys.exit(1)

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

# Add info to client to get the topi attirubutes of a given topi
client = boto3.client('sns',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
# getting attributes of a specific topic is an extension to AWS sns
pprint(client.get_topic_attributes(TopicArn=topic_arn))
