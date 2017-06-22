
from gensim import corpora, models, similarities
import pyLDAvis.gensim
import sys
from twitter_preprocessing import preprocess
import warnings



def viz(queries):
    corpus, dictionary = preprocess(queries)
    lda = models.LdaModel(corpus, id2word=dictionary, num_topics=10, passes=10)
    d = pyLDAvis.gensim.prepare(lda, corpus, dictionary)
    pyLDAvis.show(data=d)

if __name__ == "__main__":
    warnings.simplefilter("ignore")
    viz(sys.argv[1:])


