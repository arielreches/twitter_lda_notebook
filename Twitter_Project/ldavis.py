
from gensim import corpora, models, similarities
import pyLDAvis.gensim
import sys
from twitter_preprocessing_2 import preprocess
import warnings
from pymongo import MongoClient
import argparse



def connect():
    client = MongoClient()
    return client['myproject']

def get_tweets(query, tokens):
    token_id_map = db.tweets.find_one({'query': query})['token_id_map']
    id_tweet_map = dict((x[1], x[0]) for x in db.tweets.find_one({'query': query})['id_tweet_map'])
    f = open('tweets.txt', 'w')

    for data in token_id_map:
        token = data[0]
        print >> f, (token + " >> " + id_tweet_map[data[1]]).encode('utf-8')
    f.close()

    # token_id_map = db.tweets.find_one({'query': query})['token_id_map']
    # id_tweet_map = dict((x[1], x[0]) for x in db.tweets.find_one({'query': query})['id_tweet_map'])
    #
    # for token in tokens:
    #     ids = [id[1] for id in token_id_map if id[0] == token]
    #     for id in ids:
    #         print(id_tweet_map[id])



def get_dictionary_corpus(query):
    users =  db.tweets.find_one({'query': query})['tweet_data']

    dic = corpora.Dictionary([user['tokens'] for user in users if len(user['tokens']) > 0])
    print("new school", len(dic))
    corpus = [ dic.doc2bow(user['tokens']) for user in users if len(user['tokens']) > 0]
    return corpus, dic





def viz():
    d = pyLDAvis.gensim.prepare(lda, corpus, dictionary)
    pyLDAvis.show(data=d)

def get_categories():
    print(lda.show_topics())
    print("TOP TOPICS")
    print(lda.top_topics(corpus, 4))



if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("-q", "--query",
    #                     help="specify query")
    # args = parser.parse_args()

    db = connect()
    corpus, dictionary = get_dictionary_corpus("stain remover")
    corpus, dictionary = preprocess("stain remover")
    print("Old school dic", len(dictionary))
    lda = models.LdaModel(corpus, id2word=dictionary, num_topics=4, passes=20, alpha=.001)
    warnings.simplefilter("ignore")
    get_categories()
    get_tweets("stain remover", ["travel"])

    viz()



