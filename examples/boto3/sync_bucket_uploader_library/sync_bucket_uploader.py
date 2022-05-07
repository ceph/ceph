#!/usr/bin/python
import boto3
from botocore.client import Config
from json import loads
from kafka import KafkaConsumer
import threading
import time
import base64

class SyncBucketUploader:
    def __init__(self, *, rgw_endpoints, bucket_name, kafka_endpoint, access_key, secret_key, region_name):
        self._s3_clients = {}
        self._sns_clients = {}
        
        self._bucket_name = bucket_name
        
        topic_name = base64.b16encode(("Sync-" + bucket_name + "-" + kafka_endpoint).encode()).decode("utf-8")
        
        for zone in rgw_endpoints:
            self._s3_clients[zone] = boto3.client('s3',
                              endpoint_url=rgw_endpoints[zone],
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key)       

            # create a temporary sns client to create the topic we need
            # notification configurations do not sync between zones so we have to update it in every zone
            sns_client = boto3.client('sns',
                              region_name=region_name,
                              endpoint_url=rgw_endpoints[zone],
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              config=Config(signature_version='s3'))
                              
            arn = sns_client.create_topic(Name=topic_name,
                                  Attributes={"push-endpoint": "http://" + kafka_endpoint})["TopicArn"]
                                  
            notification_conf = [{'Id': 'sync-bucket-uploader',
                                  'TopicArn': arn,
                                  'Events': ['s3:ObjectSynced:*']
                                  }]

            self._s3_clients[zone].put_bucket_notification_configuration(Bucket=bucket_name,
                                                            NotificationConfiguration={
                                                                'TopicConfigurations': notification_conf})
            
        self._objects = {}
        
        thread = threading.Thread(target=lambda: self._spin(topic_name, kafka_endpoint), daemon=True)
        thread.start()
        
                
    def _spin(self, topic_name, kafka_endpoint):
        # Create new Kafka consumer to listen to the message from Ceph
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_endpoint,
            value_deserializer=lambda x: loads(x.decode("utf-8")))
    
        for msg in consumer:
            message = msg.value
            if message['s3']['bucket']['name'] == self._bucket_name and message["s3"]['object']['key'] in self._objects \
<<<<<<< HEAD
                    and message['eventName'] == "ObjectSynced":
=======
                    and message['eventName'] == "ceph:ObjectSynced":
>>>>>>> f4cb7b0defcc54218e7dd5b0893bd940dd6705a9
                key = message["s3"]['object']['key']
                if self._objects[key][0] > 0:
                    # decrement the zones we are waiting on by one
                    self._update_object_status(key, self._objects[key][0] - 1)
                    break
        
        consumer.close()
    
    def _zones_to_status(self, remaining_zones):
        if remaining_zones < 0:
            return "failed"
        elif remaining_zones == 0:
            return "success"
        return "pending"
    
    def _cancel(self, object_key):
        # if we are still waiting on a zone, thetn set status to failed
        if object_key in self._objects and self._objects[object_key][0] > 0:
            self._update_object_status(object_key, -1)
    
    def _update_object_status(self, key, remaining_zones):
        if key not in self._objects:
            self._objects[key] = [remaining_zones, None]
        else:
            self._objects[key][0] = remaining_zones
        # Run callback
        if self._objects[key][1]:
            self._objects[key][1](self._zones_to_status(remaining_zones))
    
    def set_replication_callback(self, object_key, callback):
        if object_key not in self._objects:
            self._objects[object_key] = [-1, callback]
        else:
            self._objects[object_key][1] = callback
    
    def get_replication_status(self, object_key):
        if object_key not in self._objects:
            return None
        return self._zones_to_status(self._objects[object_key][0])
    
    def upload_file(self, zone_name, file_name, object_key=None, *, timeout=-1, zones=0):
        if not object_key:
            object_key = file_name
                                                
        self._update_object_status(object_key, zones)
        
        if timeout >= 0:
            timer = threading.Timer(timeout, lambda: self._cancel(object_key))
            timer.start()
<<<<<<< HEAD
        
        try:
            # Put objects to the relevant bucket
            ans = self._s3_clients[zone_name].upload_file(Filename=file_name, Bucket=self._bucket_name,
                                        Key=object_key)
        except KeyError:
            print("Key Exception! Is it possible the provided zone does not have a client?")
=======
            
        # Put objects to the relevant bucket
        ans = self._s3_clients[zone_name].upload_file(Filename=file_name, Bucket=self._bucket_name,
                                    Key=object_key)
>>>>>>> f4cb7b0defcc54218e7dd5b0893bd940dd6705a9
