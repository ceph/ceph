import boto3
import redis
import unittest
import subprocess
import json

# Assume a redis server and RGW are running

access_key = 'test3'
secret_key = 'test3'
host = 'localhost'
port = 8000

endpoint_url = "http://%s:%d" % (host, port)

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
    
    '''
    D4N Directory Unit Tests
    '''

    # Successful setValue Call and Redis Check
    def test_set_value(self):
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        data = r.hgetall('rgw-object:test.txt:directory')

        self.assertEqual(data.get('key'), 'rgw-object:test.txt:directory')
        self.assertEqual(data.get('size'), '4')
        self.assertEqual(data.get('bucket_name'), 'bkt')
        self.assertEqual(data.get('obj_name'), 'test.txt')
        self.assertEqual(data.get('hosts'), '127.0.0.1:6379')

        r.flushall()
    
    # Successful getValue Calls and Redis Check
    def test_get_value(self):
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

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
    def test_delete_value(self):
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        response_del = obj.delete()

        self.assertEqual(response_del.get('ResponseMetadata').get('HTTPStatusCode'), 204) 
        self.assertFalse(r.exists('rgw-object:test.txt:directory'))

        r.flushall()

    '''
    D4N Cache Unit Tests: Not currently working due to lack of get_obj_attrs() call in set operation
    '''
    '''
    # Successful setObject Call and Redis Check
    def test_set_object(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        data = r.hgetall('rgw-object:test.txt:cache')
        output = subprocess.check_output(['./bin/radosgw-admin', 'object', 'stat', '--bucket=bkt', '--object=test.txt'])
        attrs = json.loads(output)
        
        self.assertEqual((data.get(b'user.rgw.tail_tag')).decode("utf-8"), attrs.get('attrs').get('user.rgw.tail_tag') + '\x00')
        self.assertEqual((data.get(b'user.rgw.pg_ver')).decode("utf-8"), attrs.get('attrs').get('user.rgw.pg_ver') + '\x00\x00\x00\x00\x00\x00\x00')
        self.assertEqual((data.get(b'user.rgw.idtag')).decode("utf-8"), attrs.get('tag') + '\x00')
        self.assertEqual((data.get(b'user.rgw.etag')).decode("utf-8"), attrs.get('etag'))
        self.assertEqual((data.get(b'user.rgw.x-amz-content-sha256')).decode("utf-8"), attrs.get('attrs').get('user.rgw.x-amz-content-sha256') + '\x00')
        self.assertEqual((data.get(b'user.rgw.source_zone')).decode("utf-8"), attrs.get('attrs').get('user.rgw.source_zone') + '\x00\x00\x00\x00')
        self.assertEqual((data.get(b'user.rgw.x-amz-date')).decode("utf-8"), attrs.get('attrs').get('user.rgw.x-amz-date') + '\x00')
        
        tmp1 = '\x08\x06L\x01\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x06\x84\x00\x00\x00\n\nj\x00\x00\x00\x03\x00\x00\x00bkt+\x00\x00\x00'
        tmp2 = '+\x00\x00\x00'
        tmp3 = '\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\x00\x00test.txt\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00!\x00\x00\x00'
        tmp4 = '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01 \x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
               '\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00default-placement\x11\x00\x00\x00default-placement\x00\x00\x00\x00\x02\x02\x18' \
               '\x00\x00\x00\x04\x00\x00\x00none\x01\x01\t\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        self.assertEqual(data.get(b'user.rgw.manifest'), tmp1.encode("latin1") + attrs.get('manifest').get('tail_placement').get('bucket').get('bucket_id').encode("utf-8")
                         + tmp2.encode("latin1") + attrs.get('manifest').get('tail_placement').get('bucket').get('bucket_id').encode("utf-8")
                         + tmp3.encode("latin1") + attrs.get('manifest').get('prefix').encode("utf-8")
                         + tmp4.encode("latin1"))

        tmp5 = '\x02\x02\x81\x00\x00\x00\x03\x02\x12\x00\x00\x00\x05\x00\x00\x00test3\x05\x00\x00\x00test3\x04\x03c\x00\x00\x00\x01\x01\x00\x00\x00\x05\x00\x00' \
               '\x00test3\x0f\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00test3\x05\x036\x00\x00\x00\x02\x02\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00test3' \
               '\x00\x00\x00\x00\x00\x00\x00\x00\x02\x02\x04\x00\x00\x00\x0f\x00\x00\x00\x05\x00\x00\x00test3\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
               '\x00\x00\x00' 
        self.assertEqual((data.get(b'user.rgw.acl')), tmp5.encode("latin1"))
       
        r.flushall()
    
    # Successful getObject Calls and Redis Check
    def test_get_object(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        data = r.hgetall('rgw-object:test.txt:cache')
        output = subprocess.check_output(['./bin/radosgw-admin', 'object', 'stat', '--bucket=bkt', '--object=test.txt'])
        attrs = json.loads(output)
        
        self.assertEqual((data.get(b'user.rgw.tail_tag')).decode("utf-8"), attrs.get('attrs').get('user.rgw.tail_tag') + '\x00')
        self.assertEqual((data.get(b'user.rgw.pg_ver')).decode("utf-8"), attrs.get('attrs').get('user.rgw.pg_ver') + '\x00\x00\x00\x00\x00\x00\x00')
        self.assertEqual((data.get(b'user.rgw.idtag')).decode("utf-8"), attrs.get('tag') + '\x00')
        self.assertEqual((data.get(b'user.rgw.etag')).decode("utf-8"), attrs.get('etag'))
        self.assertEqual((data.get(b'user.rgw.x-amz-content-sha256')).decode("utf-8"), attrs.get('attrs').get('user.rgw.x-amz-content-sha256') + '\x00')
        self.assertEqual((data.get(b'user.rgw.source_zone')).decode("utf-8"), attrs.get('attrs').get('user.rgw.source_zone') + '\x00\x00\x00\x00')
        self.assertEqual((data.get(b'user.rgw.x-amz-date')).decode("utf-8"), attrs.get('attrs').get('user.rgw.x-amz-date') + '\x00')
        
        tmp1 = '\x08\x06L\x01\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x06\x84\x00\x00\x00\n\nj\x00\x00\x00\x03\x00\x00\x00bkt+\x00\x00\x00'
        tmp2 = '+\x00\x00\x00'
        tmp3 = '\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\x00\x00test.txt\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00!\x00\x00\x00'
        tmp4 = '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01 \x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
               '\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00default-placement\x11\x00\x00\x00default-placement\x00\x00\x00\x00\x02\x02\x18' \
               '\x00\x00\x00\x04\x00\x00\x00none\x01\x01\t\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        self.assertEqual(data.get(b'user.rgw.manifest'), tmp1.encode("latin1") + attrs.get('manifest').get('tail_placement').get('bucket').get('bucket_id').encode("utf-8")
                         + tmp2.encode("latin1") + attrs.get('manifest').get('tail_placement').get('bucket').get('bucket_id').encode("utf-8")
                         + tmp3.encode("latin1") + attrs.get('manifest').get('prefix').encode("utf-8")
                         + tmp4.encode("latin1"))

        tmp5 = '\x02\x02\x81\x00\x00\x00\x03\x02\x12\x00\x00\x00\x05\x00\x00\x00test3\x05\x00\x00\x00test3\x04\x03c\x00\x00\x00\x01\x01\x00\x00\x00\x05\x00\x00' \
               '\x00test3\x0f\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00test3\x05\x036\x00\x00\x00\x02\x02\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00test3' \
               '\x00\x00\x00\x00\x00\x00\x00\x00\x02\x02\x04\x00\x00\x00\x0f\x00\x00\x00\x05\x00\x00\x00test3\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
               '\x00\x00\x00' 
        self.assertEqual((data.get(b'user.rgw.acl')), tmp5.encode("latin1"))

        # Check if object name in directory instance matches redis update
        r.hset('rgw-object:test.txt:cache', b'user.rgw.source_zone', 'source_zone_1')
        
        response_get = obj.get()
    
        self.assertEqual(response_get.get('ResponseMetadata').get('HTTPStatusCode'), 200) 
        
        data = r.hget('rgw-object:test.txt:cache', b'user.rgw.source_zone')
        
        self.assertEqual(data, b'source_zone_1')
       
        r.flushall()
   
   # Successful delObject Call and Redis Check
    def test_delete_object(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        response_del = obj.delete()

        self.assertEqual(response_del.get('ResponseMetadata').get('HTTPStatusCode'), 204) 
        self.assertFalse(r.exists('rgw-object:test.txt:cache'))

        r.flushall()
    '''

if __name__ == '__main__':
    
    unittest.main()
