from fake_operations import *
from utils import  stream_generic_data
import os, hashlib, sys
from sqlalchemy import create_engine
import pandas as pd


def get_client_hashes(mysql_args):
    host, user, pwd, _, db = [mysql_args[k] for k in ["host","user", "password","port", "db"]]
    engine_url = f"mysql+pymysql://{user}:{pwd}@{host}/{db}?charset=utf8"
    engine = create_engine(engine_url, encoding="utf8")
    query_cols_input_hash = f"SELECT agencia, conta, cpf FROM {mysql_args['table']}"
    df_client = pd.read_sql(query_cols_input_hash ,con=engine).astype('str')
    df_client["hash_input"] =  df_client[df_client.columns].agg(''.join, axis=1)
    df_client['hash_input'] = df_client['cpf'] +  df_client['agencia'] + df_client['conta']
    hashing = lambda entrada: hashlib.sha256(entrada.encode()).hexdigest()[:16]
    return df_client['hash_input'].apply(hashing).values

def contains_required_parm(parms, required):
    required_parms = [parm for parm in required if parm in parms.keys()]
    return  False if len(required_parms) != len(required) else True

def extract_not_required_parms(parms, required):
    return {parm: parms[parm] for parm in parms if parm not in required}


if __name__ == '__main__':

    kafka_args = { 
                    "host": os.getenv('KAFKA_SERVICE'),
                    "port": os.getenv('KAFKA_PORT'),
                    "topic": os.getenv('KAFKA_TOPIC')}
    mysql_args = {
                    "host": os.getenv('MYSQL_SERVICE'), 
                    "user": os.getenv('MYSQL_USER'),
                    "password":os.getenv('MYSQL_PASS'), 
                    "port": os.getenv('MYSQL_PORT'), 
                    "db": os.getenv('MYSQL_DB'),
                    "table": os.getenv('MYSQL_TABLE')
    }
    client_hashes = get_client_hashes(mysql_args)
    parms = {parm.split("=")[0]: parm.split("=")[1]  
                        for parm in sys.argv if len(parm.split("=")) > 1}
             
    if not contains_required_parm(parms, ['freq', 'method']):
        print("'freq' and 'method' are required parameters")
    else:
        frequency, method = (parms.get('freq'), locals().get(parms['method']))

        method_args = extract_not_required_parms(parms, ['freq', 'method'])
        method_args['clients'] = get_client_hashes(mysql_args)
        print(method_args)
        stream_generic_data(
                        kafka_args=kafka_args,
                        method=method,
                        method_args=method_args,
                        freq=int(frequency)
        )

    # {'clients': client_hashes, 'counterparties': client_hashes}