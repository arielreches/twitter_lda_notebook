from twython import Twython
from pymongo import MongoClient
from twitter_preprocessing_2 import preprocess


TWITTER_CONSUMER_KEY="1ILswlPvjdonAAv3WfSf3jMAQ"
TWITTER_CONSUMER_SECRET="MbDRTHMle5o7FJ8UuR0ZfE024esIcQ63NrSChzzuERp0UI7WLx"
TWITTER_ACCESS_TOKEN_KEY="876393658257481728-K6CFgaG2sPkQP88oQIGcxKCxu42E37L"
TWITTER_ACCESS_TOKEN_SECRET="SumVxycOWAejH3anLMesZXla5CIkTU4Cgl0dgDiCzNTC4"
# "generate list of query related topics, list of users, topics they're talking about"
twitter = Twython(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, oauth_version=2)
ACCESS_TOKEN = twitter.obtain_access_token()
twitter = Twython(TWITTER_CONSUMER_KEY, access_token=ACCESS_TOKEN)

client      = MongoClient()
db          = client['myproject']

def store_tweets(query):
    # get tweets from query and put in db
    count = 100
    tweet_data = twitter.search(q=query, count=count, lang="en")['statuses']
    tweet_text = ""
    id_tweet_map = []
    doc = db.tweets.find_one({"query": query})
    for idx, tw in enumerate(tweet_data):
        tweet_data[idx]['timeline_array'] = []
        user_text = ""
        tweet_text = tweet_text + tw['text'] + " "
        user_timeline = (twitter.get_user_timeline(screen_name=tw['user']['screen_name'], count=50, trim_user=True))
        for u_tw in user_timeline:
            if u_tw['lang'] in ("en", "und"):
                user_text = user_text + " " + u_tw['text']
                id_tweet_map.append((u_tw['text'], u_tw['id_str']))
                tweet_data[idx]['timeline_array'].append((u_tw['text'], u_tw['id_str']))
        tweet_data[idx]['timeline'] = user_text
    if doc == None:


        # Generate document from Tweets and store
        doc = {"count": len(tweet_data), "query": query, "tweet_data": tweet_data, "tweet_text": tweet_text, "id_tweet_map": id_tweet_map}
        tw_collection = db.tweets
        tw_collection.insert_one(doc)
    else:
        #if topic is already in databsase, add more tweets

        db.tweets.update_one(
            {"query": query},
            {"$set": {"count":  doc['count'] + len(tweet_data), "tweet_text": doc['tweet_text'] + tweet_text, "tweet_data": doc['tweet_data'] + tweet_data, "id_tweet_map": doc['id_tweet_map'] + id_tweet_map}, "$currentDate": {"lastModified": True}})


store_tweets("forrest gump")
preprocess("forrest gump")
# def combine_tweets():
#     docs = db.tweets.find()
#     for d in docs:
#         query = d['query']
#         user_tweets = d['user_tweets']
#         topic_tweets = d['tweet_text']
#         db.tweets.update_one(
#             {"query": query},
#             {"$set": {"all_tweets": user_tweets+topic_tweets}})


