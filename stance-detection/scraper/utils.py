import os
import stweet as st
from pymongo import MongoClient
import pymongo
from stweet.model.language import Language


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    username = os.environ['MONGO_INITDB_ROOT_USERNAME']
    password = os.environ['MONGO_INITDB_ROOT_PASSWORD']
    ca_path = os.environ['CA_CERTIFICATE_PATH']
    connString = f'mongodb+srv://{username}:{password}@db-mongodb-pbc-d07d8146.mongo.ondigitalocean.com/admin?authSource=admin&replicaSet=db-mongodb-pbc&tls=true&tlsCAFile={ca_path}'

    client = MongoClient(connString)

    # Create the database
    return client['pbc']


def get_proxy():
    proxy = st.DefaultTwitterWebClientProvider.get_web_client_preconfigured_for_tor_proxy(
        socks_proxy_url='socks5://torproxy:9050',
        control_host='torproxy',
        control_port=9051,
        control_password='test1234'
    )
    return proxy


def get_language(language_str: str):
    if language_str == 'pl':
        language = Language.POLISH
    elif language_str == 'en':
        language = Language.ENGLISH
    elif language_str == 'de':
        language = Language.GERMAN
    elif language_str == 'cs':
        language = Language.CZECH
    elif language_str == 'sk':
        language = Language.SLOVAK
    else:
        language = None

    return language


def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))
