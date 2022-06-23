from fake_operations import *
from utils import  stream_generic_data
import os, hashlib, sys
from sqlalchemy import create_engine
import pandas as pd
from functools import reduce

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


def main(parms, required_parms, mysql_args, kafka_args):
    if not contains_required_parm(parms, required_parms):
        return f"{reduce(lambda a, b: f'{a}, {b}', required_parms)} are required parameters"
    frequency, method, kafka_args["topic"] = \
                                (parms.get('freq'), globals().get(parms['method']), parms.get('topic'))
    method_args = extract_not_required_parms(parms, required_parms)
    method_args['clients'] = get_client_hashes(mysql_args)
    stream_generic_data(kafka_args=kafka_args, method=method, method_args=method_args, freq=int(frequency))

if __name__ == '__main__':
    kafka_args = { 
            "host": os.getenv('KAFKA_SERVICE'),"port": os.getenv('KAFKA_PORT')}

    mysql_args = {
                    "host": os.getenv('MYSQL_SERVICE'), 
                    "user": os.getenv('MYSQL_USER'),
                    "password":os.getenv('MYSQL_PASS'), 
                    "port": os.getenv('MYSQL_PORT'), 
                    "db": os.getenv('MYSQL_DB'),
                    "table": os.getenv('MYSQL_TABLE')
    }
    required_parms = ['freq', 'method', 'topic']
    parms = {parm.split("=")[0]: parm.split("=")[1]  
                        for parm in sys.argv if len(parm.split("=")) > 1}
    result = main(parms, required_parms, mysql_args, kafka_args)
    print(result)