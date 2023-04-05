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
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                endpoint_url=endpoint_url, 
                use_ssl=False,
                verify=False)

s3.create_bucket(Bucket='bkt')
obj = s3.Object(bucket_name='bkt', key='test.txt')

class D4NFilterTestCase(unittest.case.TestCase):

    @classmethod
    def setUpClass(cls):
        print("D4NFilterTest setup.")

        try:
            cls._connection = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            cls._connection.ping() 
        except:
            print("ERROR: Redis instance not running.")
            raise
    
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
        self.assertEqual(data.get('bucketName'), 'bkt')
        self.assertEqual(data.get('objName'), 'test.txt')
        self.assertEqual(data.get('hosts'), '127.0.0.1:6379')

        r.flushall()

    # Successful getValue Calls and Redis Check
    def test_get_value(self):
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 
        
        response_get = obj.get()
    
        self.assertEqual(response_get.get('ResponseMetadata').get('HTTPStatusCode'), 200) 
    
        data = r.hgetall('rgw-object:test.txt:directory')

        self.assertEqual(data.get('key'), 'rgw-object:test.txt:directory')
        self.assertEqual(data.get('size'), '4')
        self.assertEqual(data.get('bucketName'), 'bkt')
        self.assertEqual(data.get('objName'), 'test.txt')
        self.assertEqual(data.get('hosts'), '127.0.0.1:6379')

        r.flushall()
    
    # Successful copyValue Call and Redis Check
    def test_copy_value(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        # Copy to new object with 'COPY' directive; directory copy should exist and match
        m = obj.metadata
        client.copy_object(Bucket='bkt', Key='test_copy.txt', CopySource='bkt/test.txt', Metadata = m, MetadataDirective='COPY')

        self.assertEqual(r.exists('rgw-object:test_copy.txt:directory'), 1)

        data = r.hgetall('rgw-object:test.txt:directory')
        data_copy = r.hgetall('rgw-object:test_copy.txt:directory')

        self.assertEqual(len(data_copy), 5)
        self.assertEqual(data_copy[b'key'], b'rgw-object:test_copy.txt:directory')
        self.assertEqual(data_copy[b'objName'], b'test_copy.txt')
        
        data.pop(b'key')
        data.pop(b'objName')
        data_copy.pop(b'key')
        data_copy.pop(b'objName')

        self.assertEqual(data, data_copy)

        # Update object with 'REPLACE' directive; directory copy should not exist
        self.assertEqual(r.delete('rgw-object:test_copy.txt:directory'), 1)

        client.copy_object(Bucket='bkt', Key='test.txt', CopySource='bkt/test.txt', Metadata = m, MetadataDirective='REPLACE')

        self.assertEqual(r.exists('rgw-object:test_copy.txt:directory'), 0)

        r.flushall()

    # Successful delValue Call and Redis Check
    def test_delete_value(self):
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        # Ensure directory entry exists in cache before deletion
        self.assertTrue(r.exists('rgw-object:test.txt:directory'))

        response_del = obj.delete()

        self.assertEqual(response_del.get('ResponseMetadata').get('HTTPStatusCode'), 204) 
        self.assertFalse(r.exists('rgw-object:test.txt:directory'))

        r.flushall()

    '''
    D4N Cache Unit Tests
    '''

    # Successful setObject Call and Redis Check
    def test_set_object(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        data = r.hgetall('rgw-object:test.txt:cache')
        output = subprocess.check_output(['./bin/radosgw-admin', 'object', 'stat', '--bucket=bkt', '--object=test.txt'])
        attrs = json.loads(output.decode('latin-1'))

        self.assertEqual((data.get(b'user.rgw.tail_tag')), attrs.get('attrs').get('user.rgw.tail_tag').encode("latin-1") + b'\x00')
        self.assertEqual((data.get(b'user.rgw.pg_ver')), attrs.get('attrs').get('user.rgw.pg_ver').encode("latin-1") + b'\x00\x00\x00\x00\x00\x00\x00')
        self.assertEqual((data.get(b'user.rgw.idtag')), attrs.get('tag').encode("latin-1") + b'\x00')
        self.assertEqual((data.get(b'user.rgw.etag')), attrs.get('etag').encode("latin-1"))
        self.assertEqual((data.get(b'user.rgw.x-amz-content-sha256')), attrs.get('attrs').get('user.rgw.x-amz-content-sha256').encode("latin-1") + b'\x00')
        self.assertEqual((data.get(b'user.rgw.source_zone')), attrs.get('attrs').get('user.rgw.source_zone').encode("latin-1") + b'\x00\x00\x00\x00')
        self.assertEqual((data.get(b'user.rgw.x-amz-date')), attrs.get('attrs').get('user.rgw.x-amz-date').encode("latin-1") + b'\x00')
        
        tmp1 = '\x08\x06L\x01\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x06\x84\x00\x00\x00\n\nj\x00\x00\x00\x03\x00\x00\x00bkt+\x00\x00\x00'
        tmp2 = '+\x00\x00\x00'
        tmp3 = '\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\x00\x00test.txt\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00!\x00\x00\x00'
        tmp4 = '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01 \x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
               '\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00default-placement\x11\x00\x00\x00default-placement\x00\x00\x00\x00\x02\x02\x18' \
               '\x00\x00\x00\x04\x00\x00\x00none\x01\x01\t\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        self.assertEqual(data.get(b'user.rgw.manifest'), tmp1.encode("latin-1") + attrs.get('manifest').get('tail_placement').get('bucket').get('bucket_id').encode("utf-8")
                         + tmp2.encode("latin-1") + attrs.get('manifest').get('tail_placement').get('bucket').get('bucket_id').encode("utf-8")
                         + tmp3.encode("latin-1") + attrs.get('manifest').get('prefix').encode("utf-8")
                         + tmp4.encode("latin-1"))

        tmp5 = '\x02\x02\x81\x00\x00\x00\x03\x02\x12\x00\x00\x00\x05\x00\x00\x00test3\x05\x00\x00\x00test3\x04\x03c\x00\x00\x00\x01\x01\x00\x00\x00\x05\x00\x00' \
               '\x00test3\x0f\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00test3\x05\x036\x00\x00\x00\x02\x02\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00test3' \
               '\x00\x00\x00\x00\x00\x00\x00\x00\x02\x02\x04\x00\x00\x00\x0f\x00\x00\x00\x05\x00\x00\x00test3\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
               '\x00\x00\x00' 
        self.assertEqual((data.get(b'user.rgw.acl')), tmp5.encode("latin-1"))
       
        r.flushall()

    # Successful getObject Calls and Redis Check
    def test_get_object(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        response_get = obj.get()
    
        self.assertEqual(response_get.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

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

    # Successful copyObject Call and Redis Check
    def test_copy_object(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        # Copy to new object with 'COPY' directive; metadata value should not change
        obj.metadata.update({'test':'value'})
        m = obj.metadata
        m['test'] = 'value_replace'

        client.copy_object(Bucket='bkt', Key='test_copy.txt', CopySource='bkt/test.txt', Metadata = m, MetadataDirective='COPY')

        self.assertEqual(r.hexists('rgw-object:test_copy.txt:cache', b'user.rgw.x-amz-meta-test'), 0)
        
        data = r.hgetall('rgw-object:test.txt:cache')
        data_copy = r.hgetall('rgw-object:test_copy.txt:cache')

        self.assertEqual(len(data_copy), 21)

        data.pop(b'mtime') # mtime will be different
        data_copy.pop(b'mtime') # mtime will be different
        
        self.assertEqual(data, data_copy)

        # Update object with 'REPLACE' directive; metadata value should change
        client.copy_object(Bucket='bkt', Key='test.txt', CopySource='bkt/test.txt', Metadata = m, MetadataDirective='REPLACE')

        data = r.hget('rgw-object:test.txt:cache', b'user.rgw.x-amz-meta-test')

        self.assertEqual(data, b'value_replace\x00')

        r.flushall()
    
    # Successful delObject Call and Redis Check
    def test_delete_object(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        test_txt = b'test'
        response_put = obj.put(Body=test_txt)

        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200) 

        # Ensure cache entry exists in cache before deletion
        self.assertTrue(r.exists('rgw-object:test.txt:cache'))

        response_del = obj.delete()

        self.assertEqual(response_del.get('ResponseMetadata').get('HTTPStatusCode'), 204) 
        self.assertFalse(r.exists('rgw-object:test.txt:cache'))

        r.flushall()

    @classmethod
    def tearDownClass(cls):
        print("D4NFilterTest teardown.")

if __name__ == '__main__':
    
    unittest.main()
