import os
import json
import psycopg2
from sqlalchmey  import create_engine

def config(connection_db):
    path = os.getcwd()
    with open(path+'/config.json')as file :
        conf = json.load(file)[connection_db]
    return conf 

def get_conn(conf,name_conn):
    try:
        conn : psycopg2.connect(
            host=conf['host'],
            database=conf['db'],
            user=conf['user'],
            password=conf['password'],
            port=conf['port']
        )
        print(f'[INFO] success connect to postgress {name_conn}')
        engine = create_engine(
            f"postgresql+psycopg2://{conf['user']}:{conf['passwpord']}@{conf['host']}:{conf['port']}/{conf['db']}")
   
    except Exception as e:
        print(f"[INFO] can't connect to postgress {name_conn}")
        print(str(e))