# Author: Alex Perrier <alexis.perrier@gmail.com>
# License: BSD 3 clause
# Python 2.7

'''
This script takes a set of raw documents composed of twitter timelines.
Given the 'parent' twitter account, the timelines are those of the followers
of the parent account. These timelines /documents are retrieved
from a MongoDb database.
The script:
* Selects timelines that are active and with large enough content
* Cleans up the raw documents: removes URLs, stopwords, does language selection ...
* Tokenizes the documents and removes low frequency words and digits
* Stores back the tokens in the MongoDB database
* Creates and saves the resulting dictionary and Corpus in Market Matrix format
It follows on the twitter.py script where the timelines are extracted from Twitter
https://github.com/alexperrier/datatalks/blob/master/twitter/twitter.py
See also the following blog posts
* http://alexperrier.github.io/jekyll/update/2015/09/04/topic-modeling-of-twitter-followers.html
* http://alexperrier.github.io/jekyll/update/2015/09/16/segmentation_twitter_timelines_lda_vs_lsa.html
Usage:
   python twitter_tokenize.py --screen_name alexip --dbname twitter --save_dict 'data/alexip_dictionary.dict' --save_corpus 'data/alexip_corpus.mm'
'''
from __future__ import print_function
import langid
import logging
import nltk
import numpy as np
import re
import sys
import time
from collections import defaultdict
from gensim import corpora
from optparse import OptionParser
from pymongo import MongoClient
from string import digits

    # --------------------------------------
    #  Database functions
    # --------------------------------------
def preprocess(query):
    query = "Israel"
    def connect():
        client = MongoClient()
        return client['myproject']

    # Get the documents from the DB
    def get_timelines():
        tweets = db.tweets.find_one({'query': query})
        return tweets['tweet_data']

    def filter_by_length_and_lang(percent = 5, lang = ['en','und']):
        # tweets.rewind()
        # exclude 25% of documents with little content
        len_text = [ len(tw['timeline'])  for tw in tweets
                        if 'timeline' in tw.keys() and len(tw['timeline']) > 0 ]
        threshold  = np.percentile(len_text , percent)

        # tweets.rewind()
        # filter on lang and
        documents = [{ 'user_id': tw['user']['id'], 'text': tw['timeline'], 'text_array': tw['timeline_array']}
                        for tw in tweets
                        if ('lang' in tw.keys())
                            and (tw['lang'] in lang)
                            and (len(tw['timeline']) > threshold)
                    ]
        # Keep documents in English or Undefined lang and with enough content
        print ("this_many_docs", len(documents))
        return documents

    # --------------------------------------
    #  Clean documents functions
    # --------------------------------------

    def remove_urls(text):
        text = re.sub(r"(?:\@|http?\://)\S+", "", text)
        text = re.sub(r"(?:\@|https?\://)\S+", "", text)
        return text

    def doc_rm_urls():
        return [ { 'user_id': doc['user_id'], 'text': remove_urls(doc['text']), 'text_array': doc['text_array'] }
                    for doc in documents ]

    def stop_words_list():
        '''
            A stop list specific to the observed timelines composed of noisy words
            This list would change for different set of timelines
        '''
        return ['amp','get','got','hey','hmm','hoo','hop','iep','let','ooo','par',
                'pdt','pln','pst','wha','yep','yer','aest','didn','nzdt','via',
                'one','com','new','like','great','make','top','awesome','best',
                'good','wow','yes','say','yay','would','thanks','thank','going',
                'new','use','should','could','really','see','want','nice',
                'while','know','free','today','day','always','last','put','live',
                'week','went','wasn','was','used','ugh','try','kind', 'http','much',
                'need', 'next','app','ibm','appleevent','using', 'https']

    def all_stopwords():
        '''
            Builds a stoplist composed of stopwords in several languages,
            tokens with one or 2 words and a manually created stoplist
        '''
        # tokens with 1 characters
        unigrams = [ w for doc in tokenized_documents for w in doc['tokens']
                        if len(w)==1]
        # tokens with 2 characters
        bigrams  = [ w for doc in tokenized_documents for w in doc['tokens']
                        if len(w)==2]

        # Compile global list of stopwords
        stoplist  = set(  nltk.corpus.stopwords.words("english")
                        + nltk.corpus.stopwords.words("french")
                        + nltk.corpus.stopwords.words("german")
                        + stop_words_list()
                        + unigrams + bigrams)
        return stoplist

    # This returns a list of tokens / single words for each user




    def tokenize_doc():

        '''
            Tokenizes the raw text of each document
        '''

        tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+')

        new_docs = []
        for doc in documents:
            tokens = []
            ids = []
            for tweet in doc['text_array']:
                t_tokens = tokenizer.tokenize((tweet[0].lower()))
                for t in t_tokens:
                    tokens.append(t)
                    ids.append(tweet[1])
            new_docs.append(
                {'user_id': doc['user_id'],
                 'tokens': tokens,
                 'tweet_ids': ids
                 }

            )
        return new_docs
        # return [   {  'user_id': doc['user_id'],
        #                'tokens': [ ],
        #                'tweet_ids': [tweet[1] for tweet in doc['text_array'] ]
        #             }
        #             for doc in documents ]

    def count_token():
        '''
            Calculates the number of occurence of each word across the whole corpus
        '''
        token_frequency = defaultdict(int)
        for doc in tokenized_documents:
            for token in doc['tokens']:
                token_frequency[token] += 1
        return token_frequency

    def token_condition(token):
        '''
            Only keep a token that is not in the stoplist,
            and with frequency > 1 among all documents
        '''
        return  (token not in stoplist and len(token.strip(digits)) == len(token)
                            and token_frequency[token] > 1)

    def keep_best_tokens():
        '''
            Removes all tokens that do not satistify a certain condition
        '''
        return [   {  'user_id': doc['user_id'],
                       'tokens': [ token for token in doc['tokens']
                                    if token_condition(token) ],
                      'tweet_ids': [ id for i, id in enumerate(doc['tweet_ids'])
                                    if token_condition(doc['tokens'][i])]

                    }
                    for doc in tokenized_documents]

# ---------------------------------------------------------
#  Main
# ---------------------------------------------------------

# print(__doc__)
# # Display progress logs on stdout
# logging.basicConfig(level=logging.INFO,
#                     format='>>> %(asctime)s %(levelname)s %(message)s')

# ---------------------------------------------------------
#  parse commandline arguments
# ---------------------------------------------------------

# op = OptionParser()
# op.add_option("--save_dict", dest="dict_filename", default="dict.dict",
#               help="Set to the filename of the Dictionary you want to save")
#
# op.add_option("--save_corpus", dest="corpus_filename", default="corpus.mm",
#               help="Set to the filename of the Corpus you want to save")
#
#
# # Initialize
# (opts, args) = op.parse_args()
# print(opts)
#
#  MongoDB connection
    db = connect()
    tweets = get_timelines()
    dict_filename = "dict.dict"
    corpus_filename = "corpus.mm"

    # ---------------------------------------------------------
    #  Documents / timelines selection and clean up
    # ---------------------------------------------------------

    # Keep 1st Quartile of documents by length and filter out non-English words
    documents = filter_by_length_and_lang(25, ['en','und'])

    # Remove urls from each document
    documents = doc_rm_urls()

    # print("\nWe have " + str(len(documents)) + " documents in english ")
    # print()

    # ---------------------------------------------------------
    #  Tokenize documents
    # ---------------------------------------------------------

    # At this point tokenized_documents.keys() == ['user_id', 'tokens']

    tokenized_documents = tokenize_doc()

    token_frequency     = count_token()
    stoplist            = all_stopwords()
    tokenized_documents = keep_best_tokens()

    # for visualization purposes only
    for doc in tokenized_documents:
        doc['tokens'].sort()
    # ---------------------------------------------------------
    #  Save tokenized docs in database
    # ---------------------------------------------------------
    # We save the tokenized version of the raw text in the db
    tweets = db.tweets.find_one({"query": query})
    tweet_data = tweets['tweet_data']
    token_id_map = [[], []]
    for idx,twt in enumerate(tweets['tweet_data']):
        docs = [doc for doc in tokenized_documents
                    if doc['user_id'] == twt['user']['id']]
        if len(docs) == 1:
            # update existing document with tokens
            tweet_data[idx]['tokens'] = docs[0]['tokens']
            tweet_data[idx]['tweet_ids'] = docs[0]['tweet_ids']
            tweet_data[idx]['has_tokens'] = True
            tweet_data[idx]['len_tokens'] = len(docs[0]['tokens'])
            token_id_map[0] = token_id_map[0] + docs[0]['tokens']
            token_id_map[1] = token_id_map[1] + docs[0]['tweet_ids']



        else:
            # the document was not tokenized, update the record accordingly
            tweet_data[idx]['tweet_ids'] = ''
            tweet_data[idx]['tokens'] = ''
            tweet_data[idx]['has_tokens'] = False
            tweet_data[idx]['len_tokens'] = 0


        update = {'tweet_data': tweet_data, 'token_id_map': (zip(token_id_map[0], token_id_map[1]))}
        # update the record
        res = db.tweets.update_one({"query": query},
            { "$set":update }
        )

        # if res.matched_count != 1:
        #     print("unable to update record: ",
        #             str(twt['user_id']), str(twt['_id']), str(res.raw_result))

    # How many documents were tokenized?

    # print()

    # ---------------------------------------------------------
    #  Dictionary and Corpus
    # ---------------------------------------------------------

    # build the dictionary
    dictionary = corpora.Dictionary([doc['tokens'] for doc in tokenized_documents])
    dictionary.compactify()

    # We now have a dictionary with N unique tokens
    # print("Dictionary: ", end=' ')
    # print(dictionary)
    # print()

    # and save the dictionary for future use
    if dict_filename is not None:
        # print("dictionary saved in %s " % dict_filename)
        dictionary.save(dict_filename)

    # Build the corpus: vectors with occurence of each word for each document
    # and convert tokenized documents to vectors
    corpus = [  dictionary.doc2bow(doc['tokens']) for doc in tokenized_documents]

    # and save in Market Matrix format
    if corpus_filename is not None:
        # print("corpus saved in %s " % corpus_filename)
        corpora.MmCorpus.serialize(corpus_filename, corpus)

    # for i, doc in enumerate(corpus):
    #     doc.sort(key=lambda tup: tup[1])
    #     print("len", len(doc), i)
    #     for z in range(len(doc), len(doc) - 5, -1):
    #         print(i, z, dictionary.get(doc[z - 1][0]), doc[z - 1][1])

    return corpus, dictionary