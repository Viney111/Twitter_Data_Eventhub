from http.client import HTTPException
import logging
import os
from datetime import datetime
from urllib.error import HTTPError
import tweepy
import pandas as pd
import asyncio
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.storage.blob import BlobClient
import azure.functions as func


# Fetching all the Twitter API & Azure Blob Stoarge and Eventhub Keys
api_key = os.environ['tweet_api_key']
api_key_secret = os.environ['tweet_api_key_secret']
access_token = os.environ['tweet_access_token']
access_token_secret = os.environ['tweet_access_token_secret']
azure_storage_conn = os.environ['sa_asa_new']
storage_container_name = "tweetdata"
azure_blob_url_temp = os.environ['sa_asa_tweet_url']
azure_blob_url_final = os.environ['sa_asa_tweet_fin_url']
event_hub_conn_str = os.environ['event_PK']
event_hub_name = os.environ['eventhub_name']


def getting_tweets(name: str):
    """
        Description: Getting the Tweets data as per the name supplied in HTTP request
        Parameters: Taking Twitter Username.
        Output: Just fetched the data and store that twitter data into the dataframe
    """
    logging.info('Authenticating Twitter API')
    auth = tweepy.OAuthHandler(api_key,api_key_secret)
    auth.set_access_token(access_token,access_token_secret)
    api = tweepy.API(auth)
    logging.info("Getting user's public tweets")
    try:
        saved_tweets_df = pd.read_csv(azure_blob_url_final)
    except pd.errors.EmptyDataError as ex:
        public_tweets = api.user_timeline(screen_name = name,count=500,tweet_mode='extended')
    else:
        last_id = saved_tweets_df['tweet_id'].max()
        public_tweets = api.user_timeline(screen_name = name,since_id=last_id,tweet_mode='extended')
        logging.info("public tweets are fetched..")
    
    if len(public_tweets) != 0:
        tweet_data = []
        columns =['tweet_id','user_name','Tweet','ExactDate']
        for tweet in public_tweets:
            tweet_data.append([tweet.id_str, tweet.user.screen_name,tweet.full_text,tweet.created_at])
            
        new_tweets_df = pd.DataFrame(tweet_data,columns=columns)

        logging.info('Storing dataframe as csv format buffer data')
        output=new_tweets_df.to_csv(encoding = "utf-8", index=False)
    else:
        return "No new Tweets for the same"
    
    
    logging.info(f'Connecting to Azure storage account')
    blob = BlobClient.from_connection_string(
        conn_str=azure_storage_conn,
        container_name=storage_container_name,
        blob_name="tweets_temp.csv"
    )
    logging.info('Attempting to store in blob storage')
    if blob.exists():
        logging.info('Blob already exist! Attempting to delete existing blob in storage')
        blob.delete_blob(delete_snapshots="include")
        
    logging.info('Uploading tweets.csv to blob storage')
    blob.upload_blob(output)
    # asyncio.run(send_to_eventhub(output))
        
    

async def send_event_data_batch(producer: EventHubProducerClient, message: str) -> None:
    """
        Description: Sending event data batchwise
        Parameter: Eventhub producer client to send event data
        Return: None
    """
    event_data_batch = await producer.create_batch()
    event_data_batch.add(EventData(message))
    await producer.send_batch(event_data_batch)

async def send_to_eventhub(message: str) -> None:
    """
        Description: Creates Eventhub producer client and sends the message as string to EventHub.
        Parameter: The message to be sent as event data to event hub.
        Return: None
    """
    producer = EventHubProducerClient.from_connection_string(
        conn_str=event_hub_conn_str,
        eventhub_name=event_hub_name
    )
    async with producer:
        await send_event_data_batch(producer, message)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Fetching Tweets HTTP Trigger Function Triggered.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            logging.warn("No name recieved.")
            pass
        else:
            name = req_body.get('name')

    if name:
        logging.info('Name is recieved.')
        getting_tweets(name)
        return func.HttpResponse(f"Twitter data of {name} is recieved and executed successfully.")
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

