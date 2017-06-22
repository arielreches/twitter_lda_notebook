from twython import Twython
from pymongo import MongoClient


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
    doc = db.tweets.find_one({"query": query})
    tweet_text = ""
    user_tweets = ""
    for tw in tweet_data:
        tweet_text = tweet_text + tw['text'] + " "
        user_timeline = (twitter.get_user_timeline(screen_name=tw['user']['screen_name'], count=30, trim_user=True))
        for u_tw in user_timeline:
            if u_tw['lang'] in ("en", "und"):
                user_tweets = user_tweets + (u_tw['text'])

    if doc == None:
        # TODO use langid to further refine english


        # Generate document from Tweets and store
        doc = {"count": len(tweet_data), "query": query, "tweet_data": tweet_data, "tweet_text": tweet_text, "user_tweets": user_tweets, "all_tweets": user_tweets + tweet_text}
        tw_collection = db.tweets
        tw_collection.insert_one(doc)
    else:
        #if topic is already in databsase, add more tweets

        db.tweets.update_one(
            {"query": query},
            {"$set": {"count":  doc['count'] + len(tweet_data), "tweet_text": doc['tweet_text'] + tweet_text, "tweet_data": doc['tweet_data'] + tweet_data, "user_tweets": doc['user_tweets'] + user_tweets, "all_tweets": doc['user_tweets'] + user_tweets +  doc['tweet_text'] + tweet_text}, "$currentDate": {"lastModified": True}})


# def combine_tweets():
#     docs = db.tweets.find()
#     for d in docs:
#         query = d['query']
#         user_tweets = d['user_tweets']
#         topic_tweets = d['tweet_text']
#         db.tweets.update_one(
#             {"query": query},
#             {"$set": {"all_tweets": user_tweets+topic_tweets}})


