import sys
import urllib
import hmac
import hashlib
import base64
import xmltodict
import http.client
from urllib import parse as urlparse
from time import gmtime, strftime

if len(sys.argv) == 2:
    # topic arn as first argument
    topic_arn = sys.argv[1]
else:
    print ('Usage: ' + sys.argv[0] + ' <topic arn> [region name]')
    sys.exit(1)

# endpoint and keys from vstart
endpoint = '127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='


parameters = {'Action': 'GetTopic', 'TopicArn': topic_arn}
body = urlparse.urlencode(parameters)
string_date = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
content_type = 'application/x-www-form-urlencoded; charset=utf-8'
resource = '/'
method = 'POST'
string_to_sign = method + '\n\n' + content_type + '\n' + string_date + '\n' + resource 
signature = base64.b64encode(hmac.new(secret_key.encode('utf-8'), string_to_sign.encode('utf-8'), hashlib.sha1).digest()).decode('ascii')
headers = {'Authorization': 'AWS '+access_key+':'+signature,
                   'Date': string_date,
                   'Host': endpoint,
                   'Content-Type': content_type}
http_conn = http.client.HTTPConnection(endpoint)
http_conn.request(method, resource, body, headers)
response = http_conn.getresponse()
data = response.read()
status = response.status
http_conn.close()
dict_response = xmltodict.parse(data)

# getting attributes of a specific topic is an extension to AWS sns

print(dict_response, status)
