
from gensim import corpora, models, similarities, utils
import pyLDAvis.gensim
import numpy as np
import sys
import warnings
from pymongo import MongoClient
import argparse



def connect():
    client = MongoClient()
    return client['myproject']

def get_tweets(query, tokens):
    # token_id_map = db.tweets.find_one({'query': query})['token_id_map']
    # id_tweet_map = dict((x[1], x[0]) for x in db.tweets.find_one({'query': query})['id_tweet_map'])
    # f = open('tweets.txt', 'w')
    #
    # for data in token_id_map:
    #     token = data[0]
    #     print >> f, (token + " >> " + id_tweet_map[data[1]]).encode('utf-8')
    # f.close()

    token_id_map = db.tweets.find_one({'query': query})['token_id_map']
    id_tweet_map = dict((x[1], x[0]) for x in db.tweets.find_one({'query': query})['id_tweet_map'])


    for token in tokens:
        ids = [id[1] for id in token_id_map if id[0] == token]
        for id in ids:
            print(id_tweet_map[id])



def get_dictionary_corpus(query):
    users =  db.tweets.find_one({'query': query})['tweet_data']
    dic = corpora.Dictionary([users[user]['tokens'] for user in users if users[user]['has_tokens']])
    print("new school", len(dic))
    corpus = [ dic.doc2bow(users[user]['tokens']) for user in users if users[user]['has_tokens']]
    return corpus, dic





def viz():
    d = pyLDAvis.gensim.prepare(lda, corpus, dictionary)
    pyLDAvis.show(data=d)

def get_categories():
    # print(lda.show_topics(formatted=False)[0][1])
    # scores = [score[1] for score in lda.show_topics(formatted=False)]
    arrs = []
    for topic in lda.show_topics(formatted=False):
        scores = [tup[1] for tup in topic[1]]
        normal = np.linalg.norm(scores, ord=1)
        print(normal)

        normalized_topic = map(lambda tup: (tup[0], (tup[1]/normal)*100), topic[1])
        arrs.append(normalized_topic)
    print(arrs)
    print("TOP TOPICS")
    print(lda.top_topics(corpus, 10))



if __name__ == "__main__":
    warnings.simplefilter("ignore")

    # parser = argparse.ArgumentParser()
    # parser.add_argument("-q", "--query",
    #                     help="specify query")
    # args = parser.parse_args()
    db = connect()
    #
    corpus, dictionary = get_dictionary_corpus("GoPro")
    # print("new_preprocess")
    #
    # corpus, dictionary = preprocess("Coachella")
    # print("preprocessed")
    lda = models.LdaModel(corpus, id2word=dictionary, num_topics=10, passes=20, alpha=.001)
    # lda = models.LdaModel.load("ldamodel")
    get_categories()
    #
    # get_categories()
    # get_tweets("GoPro", ["incredibleindia"])

    viz()



