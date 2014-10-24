"""
This script performs topic analysis on a set of files.
"""
import topic_analysis
import cPickle
import datetime as dt
import pandas as pd
import os
import re

def buildFeatureSpace(pickle, topics, column):

    #open the featureset from a pickle as the output of mlmunge
    with open(pickle, 'rb') as some_object:
        featureset = cPickle.load(some_object)

    #DEAL WITH THIS
    def removeall_replace(x, l):
        t = [y for y in l if y != x]
        del l[:]
        l.extend(t)

    for row_index in range(len(featureset)):
        removeall_replace('nan',featureset[row_index])
        removeall_replace('nannan',featureset[row_index])

    #run the topic_analysis and return a dataframe as we want it.
    combined = topic_analysis.Corpus(featureset,column)
    combined.create_corpus()
    combined.create_tfidf()
    frame = combined.lda(num_topics = topics)

    return frame


def execute(folder, matchlist, num_topics):
    print 'Topics for {} being built.'.format(folder)
    assert len(matchlist) == len(num_topics), "Input lists must be of equal length"
    # Look up the files in the directory and find the most recently created files that meets the match list criteria
    newestfilelist = []
    for item in matchlist:
        matching = []
        for file_index in range(len(os.listdir('data/' + folder + '/'))):
            if re.findall(item,os.listdir('data/' + folder + '/')[file_index]):
                matching.append(os.path.abspath('data/' + folder + '/' + os.listdir('data/' + folder + '/')[file_index]))
        newestfilelist.append(max(matching, key = os.path.getctime))

    # Run each file identified in the match list through topic analysis. and return each dataframe to a list then save
    # to a concatenated single dataframe
    framelist = []
    for file_index in range(len(newestfilelist)):
        framelist.append(buildFeatureSpace(pickle = newestfilelist[file_index],
                          topics = num_topics[file_index], column = matchlist[file_index]))

    result = pd.concat(framelist, axis=1)

    result.to_csv('data/posttopicanalysis/{}-{}-{}-{}.csv'.format('-'.join(matchlist),
     dt.datetime.now().year, dt.datetime.now().month,
     dt.datetime.now().day), index=False, encoding='utf-8')
    print 'Topics for {} build and saved to a file in the posttopicanalysis folder'.format(folder)

    return result