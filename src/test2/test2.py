'''
Created on 2016/05/07

@author: nori
'''

import kafka
import boto3

AWS_S3_BUCKET_NAME = 'hosodatest2'

s3 = boto3.resource('s3')
bucket = s3.Bucket(AWS_S3_BUCKET_NAME)

kafka_client = kafka.KafkaClient('localhost:9092')
producer = kafka.SimpleProducer(kafka_client)
consumer = kafka.KafkaConsumer(
    'my-topic', group_id='my_group',
    bootstrap_servers=['localhost:9092'])

if __name__ == '__main__':
    for obj_summary in bucket.objects.all():
        print("key: %s " % (obj_summary.key))
        producer.send_messages('my-topic', obj_summary.key.encode("UTF-8","ignore") )
# メッセージの subscribe (for文をいきなり回せば勝手にリクエストしてくれる)
    for message in consumer:
        print("topic: %s message=%s" % (message.topic, message.value))


