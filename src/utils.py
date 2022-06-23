import argparse, time, json
from kafka import KafkaProducer
import pandas as pd
###############################################################################################
################################    KAFKA UTILS    ############################################

def get_freq_param():
    parser = argparse.ArgumentParser()
    parser.add_argument('--freq', type=int)
    args = parser.parse_args()
    return args.freq if args.freq else 1

def json_serializer(data):
    return json.dumps(data).encode('utf-8')
    
def get_kafka_producer(host, port):
    return KafkaProducer(bootstrap_servers=[f'{host}:{port}'], value_serializer=json_serializer)



def stream_generic_data(kafka_args, method, method_args, freq=1):
    producer = get_kafka_producer(kafka_args["host"], kafka_args["port"])
    while 1:
        producer.send(topic=kafka_args['topic'], value=method(method_args))
        time.sleep(1 / freq)


