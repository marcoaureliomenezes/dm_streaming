import argparse, time, json
from kafka import KafkaProducer

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
    print(f'{host}:{port}')
    return KafkaProducer(bootstrap_servers=[f'{host}:{port}'], value_serializer=json_serializer)


def stream_generic_data(topic, kafka_args, method, method_args, freq=1):
    print(kafka_args)
    producer = get_kafka_producer(**kafka_args)
    print(producer)
    while 1:
        print(method(method_args))
        producer.send(topic=topic, value=method(method_args))
        time.sleep(1 / freq)


