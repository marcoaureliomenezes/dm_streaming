from utils.fake_operations import *
from utils.kafka_utils import  stream_generic_data
import argparse


parser = argparse.ArgumentParser(description='python arguments')
parser.add_argument('--host', help= '', required=True)
parser.add_argument('--port', help= '', required=True)
parser.add_argument('--freq', help= '', required=True)
parser.add_argument('--num_clients', help= '', required=True)
args = parser.parse_args()
parm_host, parm_port, parm_freq, parm_num_clients = (
                args.host, args.port, int(args.freq), int(args.num_clients))
                

kafka_args = {"host": parm_host, "port": parm_port}

stream_generic_data(
                topic='titulos_CDB', 
                kafka_args=kafka_args,
                method=gen_investment,
                method_args={"num_clients": parm_num_clients, "tipo": "CDB"},
                freq=parm_freq
)