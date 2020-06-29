import re
import os
import json
import pprint
import string
from twython import Twython
from stop_words import get_stop_words
import sqlite3
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import numpy as np
import argparse


pp = pprint.PrettyPrinter(indent=4)

# apprendre à utiliser l'API Twitter
def connect_to_twitter(filename, search):

    with open(filename, 'r') as fp:
        credentials = json.load(fp)
        assert type(credentials) == dict, "The credentials deserialized must be a dict"

    python_tweets = Twython(credentials['consumer_key'], credentials['consumer_secret'])

    query = {
        'q' : search,
        'lang': 'fr',
        'count' : 2000
    }

    tweets = python_tweets.search(**query)['statuses']

    # save tweets
    with open('json/twitter.json', 'w') as fp:
        json.dump(tweets, fp, indent=4)

def count_words(tweet):
    # remove punctuation
    pattern = r'[\"\#\$\%\&\(\)\*\+\,\-\.\/\:\;\<\=\>\?\@\[\\\]\^\_\`\{\|\}\~\!]'
    tweet = re.sub(pattern, '', tweet)

    # separate words
    words_list = tweet.split(' ')

    # test
    assert type(words_list) == list, "Should be a list"

    return words_list, len(words_list)

def remove_stop_words(tweet):
    stop_words = get_stop_words('fr')

    assert type(stop_words) == list, "Should be a list"

    return [ word.lower() for word in tweet if word.lower() not in stop_words and word != "" and not word.startswith('http')]

def save_to_mongo(new_tweets):
    client = MongoClient("localhost", 27017)
    db = client.twitter
    tweets = db.tweet
    result = tweets.insert_many(new_tweets)

    assert len(result.inserted_ids) == len(new_tweets), "The number of ids should be the same as the number of tweets" 
    print("Tweets saved to MongoDB")

def create_table(sql_script, cur):

    with open(sql_script, "r") as fp:
        sqlFile = fp.read()

        # all SQL commands (split on ';')
        sqlCommands = sqlFile.split(';')

        # Execute every command from the input file
        for command in sqlCommands:
            try:
                cur.execute(command)
            except Exception as e:
                print("Command skipped: ", e)

def save_to_sqlite(db_file, new_tweets):
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        # CREATE TABLES if they don't exist
        create_table('sql/twitter_database_script.sql', c)

        # INSERT INTO author table
        authors_tuple = [ (author, ) for author in new_tweets['author'] ]
        c.executemany("INSERT INTO authors VALUES (?)", authors_tuple)

        # INSERT INTO tweets
        tweets_subdf = new_tweets.loc[:, ['tweet', 'date', 'nb_words']]
        tweets_subdf.loc[:, 'date'] = tweets_subdf.loc[:, 'date'].apply(lambda x : x.date())
        tweets_subdf.loc[:, 'author_id'] = new_tweets['author'].index.values.tolist()
        tweets_tuple = [ tuple(tweet_info) for tweet_info in tweets_subdf.values ]
        c.executemany("INSERT INTO tweets VALUES (?,?,?,?)", tweets_tuple)
        

        # INSERT INTO tokens
        tokens = np.hstack(new_tweets["tokens"].values)
        tokens = set(tokens.tolist())
        tokens = [ (token, ) for token in tokens ]
        c.executemany("INSERT INTO tokens VALUES (?)", tokens)


        # INSERT INTO tokens_by_article
        join_table = []

        for idx_tweet, tweet in enumerate(new_tweets['tweet']):
            for idx_token, token in enumerate(tokens):
                if token[0] in tweet:
                    join_table.append((idx_tweet, idx_token))

        c.executemany("INSERT INTO tokens_by_article VALUES (?,?)", join_table)

        conn.commit()
        print('Tweets saved to SQLite')

    except Exception as e:
        print('issue : ', e)
    finally:
        if conn:
            conn.close()

def get_relevant_data(tweet):
    # count words + tokenization
    tweet_list, tweet_words_count = count_words(tweet['text'])
    tokens = remove_stop_words(tweet_list)

    tweet_dict = dict()
    tweet_dict["author"] = tweet['user']['name']
    tweet_dict["date"] = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
    tweet_dict["tweet"] = tweet['text']
    tweet_dict["nb_words"] = tweet_words_count
    tweet_dict["tokens"] = tokens

    return tweet_dict

def fetch_tweets(credentials_file = None, search = None):

    if credentials_file is None or not credentials_file:
        raise Exception("the credentials filename can't be None nor empty")

    if search is None or not search:
        raise Exception("the query can't be None or empty")
    
    # fetch tweets with the desired query
    if not os.path.isfile('./json/twitter.json'):
        connect_to_twitter(credentials_file, search)

    # load tweets saved in JSON file
    try:
        with open("json/twitter.json", "r") as fp:
            tweets = json.load(fp)
            assert type(tweets) == list, "Json deserialized should be a list of JSON"
    except Exception as e:
        print(e)

    # get the author, tweet, date, nb words and tokens of each tweet
    tweets_to_save = [get_relevant_data(tweet) for tweet in tweets]

    # save to sqlite
    save_to_sqlite('sql/twitter_tweets.db', pd.DataFrame(tweets_to_save))

    # save to mongo
    save_to_mongo(tweets_to_save)

    # ================= for debug only ====================
    # pp.pprint(tweets_to_save[0].values())


# MAIN APP
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Use of Twitter API and save data to MongoDB and SQLite')
    parser.add_argument('--credentials', required=True)
    parser.add_argument('--search', required=True)
    args = parser.parse_args()

    fetch_tweets(credentials_file = f'json/{args.credentials}', search = args.search)


