import boto3
import redis
import unittest

# Assume a redis server and RGW are running

access_key = 'test3'
secret_key = 'test3'
host = 'localhost'
port = 8000

endpoint_url = "http://%s:%d" % (host, port)

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

client = boto3.client(service_name='s3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                endpoint_url=endpoint_url,
                use_ssl=False,
                verify=False)

s3 = boto3.resource('s3', 
                use_ssl=False,
                verify=False,
                endpoint_url=endpoint_url, 
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key)

bucket = s3.Bucket('bkt')
bucket_location = bucket.create()
obj = s3.Object(bucket_name='bkt', key='test.txt')

class D4NFilterTestCase(unittest.TestCase):
    
    def test_set_get(self):
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)
        response_get = obj.get()

        res_put = response_put.get('ResponseMetadata')
        res_get = response_get.get('ResponseMetadata')
        
        self.assertEqual(res_put.get('HTTPStatusCode'), 200) 
        self.assertEqual(res_get.get('HTTPStatusCode'), 200) 

        data = r.hgetall('test.txt')

        self.assertEqual(data.get('key'), 'test.txt')
        self.assertEqual(data.get('size'), '4')
        self.assertEqual(data.get('bucket_name'), 'bkt')
        self.assertEqual(data.get('obj_name'), 'test.txt')
        self.assertEqual(data.get('hosts'), '127.0.0.1:6379')
        
    def test_delete(self):
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)
        response_get = obj.get()

        res_put = response_put.get('ResponseMetadata')
        res_get = response_get.get('ResponseMetadata')

        self.assertEqual(res_put.get('HTTPStatusCode'), 200) 
        self.assertEqual(res_get.get('HTTPStatusCode'), 200) 

        s3.Object('bkt', 'test.txt').delete()

        self.assertFalse(r.exists('test.txt'))

if __name__ == '__main__':
    
    unittest.main()
