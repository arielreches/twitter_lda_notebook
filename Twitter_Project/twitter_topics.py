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
MAX_ID_INIT_DEFAULT = 8999999999999999999
client      = MongoClient()
db          = client['myproject']
unique_accounts = []
final_max_id = MAX_ID_INIT_DEFAULT
query = "Trump"


def get_unique_users(init_max_id, initial_accounts):
    global unique_accounts
    global final_max_id
    accounts = initial_accounts
    max_id = init_max_id
    for i in range(0, 10):
        count = 30
        tweet_data = twitter.search(q=query, count=count, lang="en", max_id=max_id)['statuses']
        for idx, tw in enumerate(tweet_data):
            if int(tw['id'] < max_id):
                max_id = tw['id']
            accounts.append(tw['user']['screen_name'])
        print(len(set(accounts)))
    print("initial accounts", len(initial_accounts))
    unique_accounts = set(accounts) - set(initial_accounts)


    final_max_id = max_id

doc = db.tweets.find_one({"query": query})
if doc == None:
    print("starting fresh")
    init_max_id = MAX_ID_INIT_DEFAULT
    initial_accounts = []
else:
    init_max_id = doc['max_id']
    initial_accounts = doc['unique_accounts']
print(len(initial_accounts))
get_unique_users(init_max_id, initial_accounts)
print "added accounts", len(unique_accounts)

accounts_object = {}
id_tweet_map = []
for idx, user in enumerate(unique_accounts):
    accounts_object[user] = {}
    accounts_object[user]['timeline_array'] = []
    user_text = ""
    user_timeline = (twitter.get_user_timeline(screen_name=user, count=20, trim_user=True))

    for u_tw in user_timeline:
        if u_tw['lang'] in ("en", "und"):
            user_text = user_text + " " + u_tw['text']
            id_tweet_map.append((u_tw['text'], u_tw['id_str']))
            accounts_object[user]['timeline_array'].append((u_tw['text'], u_tw['id_str']))
    accounts_object[user]['timeline'] = user_text

print "this many timelines", len(accounts_object)
    # Generate document from Tweets and store
if doc == None:
    doc = {"unique_accounts": list(unique_accounts), "query": query, "tweet_data": accounts_object, "id_tweet_map": id_tweet_map, "max_id": final_max_id}
    tw_collection = db.tweets
    tw_collection.insert_one(doc)
else:
    #if topic is already in databsase, add more tweets

    db.tweets.update_one(
        {"query": query},
        {"$set": {"unique_accounts":  doc['unique_accounts'] + list(unique_accounts), "tweet_data": dict(accounts_object.items() + doc['tweet_data'].items()),   "id_tweet_map": id_tweet_map + doc['id_tweet_map'], "max_id": final_max_id}, "$currentDate": {"lastModified": True}})






# def store_tweets(query):
#     # get tweets from query and put in db
#     unique_accounts = count_unique_users(query)
#     count = 100
#     tweet_data = twitter.search(q=query, count=count, lang="en")['statuses']
#     tweet_text = ""
#     id_tweet_map = []
#     doc = db.tweets.find_one({"query": query})
#     for idx, tw in enumerate(tweet_data):
#         tweet_data[idx]['timeline_array'] = []
#         user_text = ""
#         tweet_text = tweet_text + tw['text'] + " "
#         user_timeline = (twitter.get_user_timeline(screen_name=tw['user']['screen_name'], count=50, trim_user=True))
#         for u_tw in user_timeline:
#             if u_tw['lang'] in ("en", "und"):
#                 user_text = user_text + " " + u_tw['text']
#                 id_tweet_map.append((u_tw['text'], u_tw['id_str']))
#                 tweet_data[idx]['timeline_array'].append((u_tw['text'], u_tw['id_str']))
#         tweet_data[idx]['timeline'] = user_text
#     if doc == None:
#
#
#         # Generate document from Tweets and store
#         doc = {"count": len(tweet_data), "query": query, "tweet_data": tweet_data, "tweet_text": tweet_text, "id_tweet_map": id_tweet_map}
#         tw_collection = db.tweets
#         tw_collection.insert_one(doc)
#     else:
#         #if topic is already in databsase, add more tweets
#
#         db.tweets.update_one(
#             {"query": query},
#             {"$set": {"count":  doc['count'] + len(tweet_data), "tweet_text": doc['tweet_text'] + tweet_text, "tweet_data": doc['tweet_data'] + tweet_data, "id_tweet_map": doc['id_tweet_map'] + id_tweet_map}, "$currentDate": {"lastModified": True}})








# store_tweets("Coachella")
# store_tweets("Coachella")
# preprocess("Coachella")
# def combine_tweets():
#     docs = db.tweets.find()
#     for d in docs:
#         query = d['query']
#         user_tweets = d['user_tweets']
#         topic_tweets = d['tweet_text']
#         db.tweets.update_one(
#             {"query": query},
#             {"$set": {"all_tweets": user_tweets+topic_tweets}})


