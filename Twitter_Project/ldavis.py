
from gensim import corpora, models, similarities
import pyLDAvis.gensim
import sys
from twitter_preprocessing_2 import preprocess
import warnings
from pymongo import MongoClient


def connect():
    client = MongoClient()
    return

def get_tweets(token):
    tweets = []
    token_id_map = db.tweets.find_one({'query': "Israel"})['token_id_map']
    ids = [item[1] for item in token_id_map if item[0] == token]





def viz():
    d = pyLDAvis.gensim.prepare(lda, corpus, dictionary)
    pyLDAvis.show(data=d)

def get_categories():
    print(lda.show_topics())
    print("TOP TOPICS")
    print(lda.top_topics(corpus, 4))



if __name__ == "__main__":
    db = connect()
    corpus, dictionary = preprocess(sys.argv[1:])
    lda = models.LdaModel(corpus, id2word=dictionary, num_topics=4, passes=20, alpha=.001)
    warnings.simplefilter("ignore")
    get_categories()


