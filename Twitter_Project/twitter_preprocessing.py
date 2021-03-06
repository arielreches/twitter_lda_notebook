import langid
import nltk
import re
import time
from collections import defaultdict
from configparser import ConfigParser
from gensim import corpora, models, similarities
from nltk.tokenize import RegexpTokenizer
from pymongo import MongoClient
from string import digits
import sys
from twitter_topics import store_tweets




def filter_lang(lang, documents):
    doclang = [  langid.classify(doc) for doc in documents ]
    return [documents[k] for k in range(len(documents)) if doclang[k][0] == lang]

def preprocess(queries):

    # connect to the MongoDB
    client      = MongoClient()
    db          = client['myproject']

    # Load documents and followers from db
    # Filter out non-english timelines and TL with less than 2 tweets

    documents = []
    for query in queries:
        doc = db.tweets.find_one({"query": query})
        if doc == None:
            store_tweets(query)
            doc = db.tweets.find_one({"query": query})
        for timeline_tweets in doc['user_tweets']:
            documents.append(timeline_tweets)

    # documents = [tw['tweet_text'] for tw in db.tweets.find()
    #              if ('lang' in tw.keys()) and (tw['lang'] in ('en', 'und'))
    #              and ('count' in tw.keys()) and (tw['count'] > 2)]


    #  Filter non english documents
    documents = filter_lang('en', documents)
    print("We have " + str(len(documents)) + " documents in english ")

    # Remove urls
    documents = [re.sub(r"(?:\@|http?\://)\S+", "", doc)
                    for doc in documents ]

    # Remove documents with less 100 words (some timeline are only composed of URLs)


    # tokenize
    from nltk.tokenize import RegexpTokenizer

    tokenizer = RegexpTokenizer(r'\w+')
    documents = [ tokenizer.tokenize(doc.lower()) for doc in documents ]

    # Remove stop words
    stoplist_tw=['amp','get','got','hey','hmm','hoo','hop','iep','let','ooo','par',
                'pdt','pln','pst','wha','yep','yer','aest','didn','nzdt','via',
                'one','com','new','like','great','make','top','awesome','best',
                'good','wow','yes','say','yay','would','thanks','thank','going',
                'new','use','should','could','best','really','see','want','nice',
                'while','know', 'https']

    unigrams = [ w for doc in documents for w in doc if len(w)==1]
    bigrams  = [ w for doc in documents for w in doc if len(w)==2]

    stoplist  = set(nltk.corpus.stopwords.words("english") + stoplist_tw
                    + unigrams + bigrams)
    documents = [[token for token in doc if token not in stoplist]
                    for doc in documents]

    # rm numbers only words
    documents = [ [token for token in doc if len(token.strip(digits)) == len(token)]
                    for doc in documents ]

    # Lammetization
    # This did not add coherence ot the model and obfuscates interpretability of the
    # Topics. It was not used in the final model.
    #   from nltk.stem import WordNetLemmatizer
    #   lmtzr = WordNetLemmatizer()
    #   documents=[[lmtzr.lemmatize(token) for token in doc ] for doc in documents]

    # Remove words that only occur once
    token_frequency = defaultdict(int)

    # count all token
    for doc in documents:
        for token in doc:
            token_frequency[token] += 1

    # keep words that occur more than once
    documents = [ [token for token in doc if token_frequency[token] > 1]
                    for doc in documents  ]

    documents = [doc for doc in documents if len(doc) > 100]


    # Sort words in documents
    for doc in documents:
        doc.sort()

    # Build a dictionary where for each document each word has its own id
    dictionary = corpora.Dictionary(documents)
    dictionary.compactify()
    # and save the dictionary for future use
    # dictionary.save(''.join(queries) + '.dict')

    # We now have a dictionary with 26652 unique tokens

    # Build the corpus: vectors with occurence of each word for each document
    # convert tokenized documents to vectors
    corpus = [dictionary.doc2bow(doc) for doc in documents]
    for i,doc in enumerate(corpus):
        doc.sort(key=lambda tup: tup[1])
        print("len", len(doc), i)
        for z in range (len(doc),len(doc)-5, -1 ):

            print(i,z, dictionary.get(doc[z-1][0]), doc[z-1][1])


        # and save in Market Matrix format
    # corpora.MmCorpus.serialize(''.join(queries) + '_corpus.mm', corpus)
    return corpus, dictionary
        # this corpus can be loaded with corpus = corpora.MmCorpus('alexip_followers.mm')
