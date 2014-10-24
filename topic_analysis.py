from gensim import corpora, models
import logging
import pandas as pd
import numpy as np

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class Corpus():
    def __init__(self, input, name):
            print "Wait.."
            self.name = name
            self.input = input
            print "Corpus of length : {} successfully created.".format(len(self.input))

    def remove_singles(self):
        """
        Removes words from a document that only occur once.  This is good for larger document corpus.
        :return: NA
        """
        all_tokens = sum(self.object, [])
        tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
        x.object = [[word for word in text if word not in tokens_once]
                 for text in self.object]

    def size(self):
        """
        Returns the total number of words in a corpus with duplicates counted.
        :return:
        """
        total_size = 0
        for row in self.object:
            total_size += len(row)
        return total_size

    def rows(self):
        """
        returns the number of documents (rows) in a corpus.
        :return:
        """
        return len(self.object)

    def create_corpus(self):
        """
        Generates a corpus
        :return: NA
        """
        self.dictionary = corpora.Dictionary(self.input)
        self.dictionary.save('data/dictionaries/'+ str(self.name) +'.dict')
        self.corpus = [self.dictionary.doc2bow(text) for text in self.input]

    def write_corpus(self,filename):
        """
        Writes a corpus to a file of the users choosing.
        :param filename: the name pf the file
        :return: NA
        """
        corpora.MmCorpus.serialize(filename, corpus)

    def create_tfidf(self):
        tfidf = models.TfidfModel(self.corpus)
        self.corpus_tfidf = tfidf[self.corpus]

    def print_tfidf(self):
        """
        Prints the weighted values for all words in the corpus for each document.  Each document represents a row.
        :return:
        """
        for doc in self.corpus_tfidf:
            print(doc)

    def lda(self,num_topics):
        """
        Performs Latent Dirichlet Allocation on a corpus and returns a dataframe ready to be added to a feature set for
        machine learning.
        :param num_topics:
        :return: A dataframe of topic relevance by document resulting from LSI.
        """
        #create dictionary and deal with tf-idf
        lda = models.LdaModel(self.corpus_tfidf, id2word=self.dictionary, num_topics=num_topics)
        corpus_lda = lda[self.corpus_tfidf]
        lda.save('data/dictionaries/'+ str(self.name))

        lda_frame = self.__get_dataframe(num_topics = num_topics,corpus = corpus_lda)


        return lda_frame

    def lsi(self,num_topics):
        """
        Performs Latent Semantic Indexing on a corpus and returns a dataframe ready to be added to a feature set for
        machine learning.
        :param num_topics:
        :return: A dataframe of topic relevance by document resulting from LSI.
        """
        #create dictionary and deal with tf-idf
        lsi = models.LsiModel(self.corpus_tfidf, id2word=self.dictionary, num_topics=num_topics)
        corpus_lsi = lsi[self.corpus_tfidf]
        lsi.save('data/dictionaries/'+ str(self.name))

        lsi_frame = self.__get_dataframe(num_topics = num_topics,corpus = corpus_lsi)

        return lsi_frame

    def __get_dataframe(self, num_topics, corpus):
        """
        Private function used to generate a dataframe of all topic similarities. Creating topics as columns and
        documents as rows.  Each cell represents a similarity coefficient.
        :param num_topics: The number of topics the user wants to generate.
        :param corpus: A list of tuples for each document giving a unique identifier for a word in the [0] position
        and the weight in the [1] position
        :return: A dataframe of topic relevance by document.
        """
        temp_list = []
        for doc in corpus:
            result_list = []
            for topic_index in range(num_topics):
                try:
                    result_list.append(doc[topic_index][1])
                except:
                    result_list.append(0.0)
            temp_list.append(result_list)

        topics_list = []
        for number in range(num_topics):
            topics_list.append(self.name + '-topic-' + str(number))

        frame = pd.DataFrame(np.array(temp_list))
        frame.columns = topics_list

        return frame
