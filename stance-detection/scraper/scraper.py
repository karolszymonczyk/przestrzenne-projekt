from os import error
import stweet as st
from arrow import Arrow
import pandas as pd
from stweet.http_request.web_client import WebClient
from pymongo.database import Database
from stweet.model.language import Language
from stweet.raw_output.raw_data_output import RawDataOutput, RawData
from typing import List
from pymongo import UpdateOne
import json

from stweet.search_runner.replies_filter import RepliesFilter
from utils import get_database, get_language, get_proxy, chunks
import time


class MongoDBRawOutput(RawDataOutput):
    col_name: str
    db: Database

    def __init__(self, col_name: str, db: Database):
        self.col_name = col_name
        self.db = db

    def save_data(self, requests: List[UpdateOne]):
        if len(requests) > 0:
            self.db[self.col_name].bulk_write(requests)


class TweetsRawOutput(MongoDBRawOutput):
    def export_raw_data(self, raw_data_list: List[RawData]):
        requests = []
        for raw in raw_data_list:
            data = json.loads(raw.raw_value)
            data['download_datetime'] = pd.to_datetime(
                raw.download_datetime.format())
            data['created_at'] = pd.to_datetime(data['created_at'])
            requests.append(UpdateOne({'id_str': data['id_str']}, {
                            '$set': data}, upsert=True))
        self.save_data(requests)


class UsersRawOutput(MongoDBRawOutput):
    def export_raw_data(self, raw_data_list: List[RawData]):
        requests = []
        for raw in raw_data_list:
            data = json.loads(raw.raw_value)
            data['download_datetime'] = pd.to_datetime(
                raw.download_datetime.format())
            data['created_at'] = pd.to_datetime(data['created_at'])
            if 'screen_name' in data:
                requests.append(UpdateOne({'screen_name': data['screen_name']}, {
                                '$set': data}, upsert=True))
        self.save_data(requests)


def scrape_tweets(since: Arrow, until: Arrow, language: Language, any_word: str or None, all_words: str or None, db: Database, usernames: List[str] or None = None, single_period: int = 60 * 60 * 24):
    collector = st.CollectorRawOutput()
    tweets_output = TweetsRawOutput('tweets', db)
    users_output = UsersRawOutput('users', db)
    web_client = get_proxy()

    if usernames is None or len(usernames) == 0:
        usernames = [None]

    tweets_count = 0
    total_seconds = int((until - since).total_seconds())
    for shift in range(0, total_seconds, single_period):
        start_date = since.shift(seconds=shift)
        end_date = since.shift(seconds=shift + single_period)

        period_tweets_count = 0
        for username in usernames:
            if username:
                print(f'Scraping for {username}')

            search_tweets_task = st.SearchTweetsTask(
                from_username=username,
                any_word=any_word,
                all_words=all_words,
                since=start_date,
                until=end_date,
                language=language,
                replies_filter=RepliesFilter.ONLY_ORIGINAL
            )

            result = None
            tries = 0
            while tries < 5 and result is None:
                try:
                    result = st.TweetSearchRunner(
                        search_tweets_task=search_tweets_task,
                        tweet_raw_data_outputs=[collector, tweets_output],
                        user_raw_data_outputs=[collector, users_output],
                        web_client=web_client
                    ).run()
                except error:
                    print('Something went wrong. Retrying...')
                    tries += 1
                    time.sleep(6)
                    web_client = get_proxy()

            if result is None:
                raise ConnectionError('Retries exceeded limit')

            tweets_count += result.downloaded_count
            period_tweets_count += result.downloaded_count
        print(f'({start_date}) -> {period_tweets_count} tweets')

    return tweets_count


if __name__ == '__main__':
    f = open('./scraper/scraper_config.json')
    config = json.load(f)

    config['since'] = Arrow(**config['since'])
    config['until'] = Arrow(**config['until'])
    config['language'] = get_language(config.get('language', None))
    chunk_size = config.pop('all_words_chunk_size', 5)

    word_chunks = list(chunks(config.pop('any_word', []), chunk_size))
    all_words = ' '.join(config.pop('all_words', []))

    # Append the list with an empty string in order to execute
    # the for loop at least once
    if len(word_chunks) == 0:
        word_chunks.append([''])

    for chunk in word_chunks:
        any_word = ' '.join(chunk)
        print(f'Scraping tweets for "{any_word}"  (only original)...')
        downloaded_count = scrape_tweets(
            **config, all_words=all_words, any_word=any_word, db=get_database())
        print(f'Downloaded {downloaded_count} tweets for "{any_word}"!')
