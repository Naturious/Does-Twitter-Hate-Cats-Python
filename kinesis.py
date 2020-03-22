import tweepy
from config import create_api
import boto3

import datetime
import logging

import json
import re

import signal
import sys

# Setup listener for exiting
def signal_handler(sig, frame):
    print('You pressed Ctrl+C, exiting..')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler) 

# Setup logging object
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def dateConverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

client = boto3.client('kinesis')

# Listener class for listening on the stream
class Listener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        self.me = api.me()

    def on_status(self, tweet):
        if(tweet.text):

            logger.info(f"Processing tweet id {tweet.id}")
            
            record = json.dumps({
                'id':tweet.id,
                'timestamp':tweet.created_at,
                'tweet': re.sub(r"[\"'}{|]", '',tweet.text)
                }, separators=(',', ':'), default=dateConverter) + '|'
            
            logger.info(record)

            response = client.put_record(
                StreamName='twitterStream',
                Data=record.encode(), # transform into Bytes stream 
                PartitionKey='key'
            )
            print(response)


    def on_error(self, status):
        logger.error(status)

# Main function that will run when we start the program
api = create_api()
tweets_listener = Listener(api)
stream = tweepy.Stream(api.auth, tweets_listener)
stream.filter(track=["cat"], languages=["en"])