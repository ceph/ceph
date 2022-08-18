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
    
    # Successful setValue Call and Redis Check
    def test_set(self):
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        res_put = response_put.get('ResponseMetadata')
        
        self.assertEqual(res_put.get('HTTPStatusCode'), 200) 

        data = r.hgetall('rgw-object:test.txt:directory')

        self.assertEqual(data.get('key'), 'rgw-object:test.txt:directory')
        self.assertEqual(data.get('size'), '4')
        self.assertEqual(data.get('bucket_name'), 'bkt')
        self.assertEqual(data.get('obj_name'), 'test.txt')
        self.assertEqual(data.get('hosts'), '127.0.0.1:6379')

        r.flushall()
    
    # Successful getValue Calls and Redis Check
    def test_get(self):
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        res_put = response_put.get('ResponseMetadata')
        
        self.assertEqual(res_put.get('HTTPStatusCode'), 200) 

        data = r.hgetall('rgw-object:test.txt:directory')

        self.assertEqual(data.get('key'), 'rgw-object:test.txt:directory')
        self.assertEqual(data.get('size'), '4')
        self.assertEqual(data.get('bucket_name'), 'bkt')
        self.assertEqual(data.get('obj_name'), 'test.txt')
        self.assertEqual(data.get('hosts'), '127.0.0.1:6379')

        # Check if object name in directory instance matches redis update
        r.hset('rgw-object:test.txt:directory', 'obj_name', 'newoid')
        
        response_get = obj.get()
    
        res_get = response_get.get('ResponseMetadata')
    
        self.assertEqual(res_get.get('HTTPStatusCode'), 200) 
    
        data = r.hget('rgw-object:test.txt:directory', 'obj_name')
        
        self.assertEqual(data, 'newoid')

        r.flushall()
   
    # Successful delValue Call and Redis Check
    def test_delete(self):
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        res_put = response_put.get('ResponseMetadata')

        self.assertEqual(res_put.get('HTTPStatusCode'), 200) 

        s3.Object('bkt', 'rgw-object:test.txt:directory').delete()

        self.assertFalse(r.exists('rgw-object:test.txt:directory'))

        r.flushall()

if __name__ == '__main__':
    
    unittest.main()
