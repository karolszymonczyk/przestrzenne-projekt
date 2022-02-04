import os
from pymongo import MongoClient, database
import dotenv
import jsonlines
import argparse
from tqdm import tqdm


def get_database() -> database.Database:
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    username = os.environ['MONGO_INITDB_ROOT_USERNAME']
    password = os.environ['MONGO_INITDB_ROOT_PASSWORD']
    ca_path = os.environ['CA_CERTIFICATE_PATH']
    connString = f'mongodb+srv://{username}:{password}@db-mongodb-pbc-d07d8146.mongo.ondigitalocean.com/admin?authSource=admin&replicaSet=db-mongodb-pbc&tls=true&tlsCAFile={ca_path}'

    client = MongoClient(connString)

    # Create the database
    return client['pbc']


def download_data(db: database.Database, col: str, filepath: str, fields: list[str]) -> None:
    projection = {col: 1 for col in fields}
    projection['_id'] = 0
    res = db[col].find({}, projection)
    count = db[col].count_documents({})
    with jsonlines.open(filepath, mode='w') as writer:
        for post in tqdm(res, total=count):
            post['created_at'] = post['created_at'].isoformat()
            post['download_datetime'] = post['download_datetime'].isoformat()
            writer.write(post)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download tweets or users from the remote DB and save it to a JSONLines file.')
    parser.add_argument('--output', required=True, type=str)
    parser.add_argument('--col', choices=['tweets', 'users'], default='tweets')
    parser.add_argument('--env', type=str, default='../.env')


    args = parser.parse_args()
    dotenv.load_dotenv(dotenv_path=args.env) 

    tweets_fields = ['id_str', 'created_at', 'download_datetime', 'full_text', 'in_reply_to_screen_name', 'lang',
                     'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'user_id_str']
    users_fields = ['id_str', 'created_at', 'download_datetime', 'name', 'screen_name', 'lang',
                    'description', 'favourites_count', 'followers_count', 'friends_count', 'location', 'media_count']

    db = get_database()
    print('Downloading data...')
    download_data(db, args.col, args.output,
                  tweets_fields if args.col == 'tweets' else users_fields)
