# # This script will tokenize everything I get in the dataframe that I want to tokenize

import pandas as pd
import numpy as np
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
import nltk
import re
from itertools import *
import datetime as dt
import csv
import cPickle
import os


def cleanflattext(framecol,colname):
    """
    This function expects a dataframe column or a list of unstructured text.

    For Example:
    ['She's a man eater.','The dog jumped over the brown log.','I would like to have a brown dog?']
    """
    #framecol = features.cdescription
    rawtextrows = list(framecol)
    #colname = 'cdescription'

    #take out bad formatting#
    print "REGEX Starting for {}".format(colname)
    textrows = [0] * len(rawtextrows)
    #make sure all rows are strings.
    textrows = map(lambda c: str(c),rawtextrows)
    textrows = map(lambda c: re.sub('\snan\s',' ', c),textrows)
    #decode the entire string into utf-8 for each row.
    textrows = map(lambda d: d.decode('utf-8'),textrows)
    #remove words that are found in the middle of a sentence and are capitalized. Remove the \r's and \n's found throughout.
    #remove digits.
    textrows = map(lambda e: re.sub('\s[A-Z]\w+|\r|\n|\d+',' ', e),textrows)
    #try to remove basic web addresses starting with www.
    textrows = map(lambda e: re.sub('w{3}.\w+.\\b',' ', e),textrows)
    #remove all non-alphanumeric characters no matter the length
    textrows = map(lambda e: re.sub('\W+',' ', e),textrows)
    #make all words lowercase
    textrows = map(lambda f: f.lower(),textrows)
    #get rid of words that are under 3 letters long.
    textrows = map(lambda e: re.sub('(\s|^)(\S{0,2}(\s|$))+',' ', e),textrows)
    #get rid of words that have three letters in a row
    textrows = map(lambda e: re.sub('([a-z])\1{2}\b',' ', e),textrows)

    print "REGEX Complete for {}".format(colname)
    #english mask generates a list of indexes (rows) that are companies that are speaking in english.
    english_mask = []
    #A list of regex enabled words that are used to positively identify english
    if colname == 'cdescription':
        english_indicators = '(\s|^)(our|assets|market|based|founded|and|industry|the|with)(\s|$)'
        english_mask,delete_indexes = [], []
        print "English Detection Starting for {}".format(colname)
        for row_index in range(len(textrows)):
            success = re.search(english_indicators, textrows[row_index])
            if success:
                english_mask.append(row_index)
        textrows = [textrows[element] for element in english_mask]
        print "English Detection Complete for {}".format(colname)
    else:
        delete_indexes = 0

    #import the nltk stopwords list plus add some of my own words into the stopword list.
    my_stopwords = stopwords.words('english') + ['com','senior','vice','president','analyst','senior','junior','director',
                                                 'manager','supervisor','chief','deputy','ceo','coo','cp','cfo','cio',
                                                 'team','teaming','synergy','customer','service','services','talent',
                                                 'help','employer','world','range','venture','joint','join','business',
                                                 'company','success','solution','deliver','experience','information',
                                                 'priority','year','trillion','billion','million','thousand','client',
                                                 'asset','known','full','establish','culture','profit','non','year',
                                                 'month','nationwide','regional','state','proud','better','best','great',
                                                 'known','place','every','everything','big','small','trust','current',
                                                 'product','deliver','well','percent','foundation','together','built',
                                                 'work','make','synergy','brand','headquarter','companies','strong',
                                                 'solid','dedicate','dedication','acquire','trademark','surround',
                                                 'quick','welcome','profession','professional']

    print "Stopword removal Starting for {}".format(colname)
    preregex = '|'.join(my_stopwords)
    stopwords_regex = '(\s|^)((' + preregex + ')(\s|$))+'
    success = map(lambda f: re.sub(stopwords_regex,' ',f), textrows)
    print "Stopword removal Complete for {}".format(colname)


    snowball_st = SnowballStemmer("english")

    stemmedresult = ['o'] * len(success)
    tokenized = ['o'] * len(success)
    print "Tokenization Starting for {}".format(colname)

    for row_index in range(len(success)):
        tokenized[row_index] = nltk.word_tokenize(success[row_index])
    print "Tokenization Complete for {}".format(colname)

    for row_index in range(len(tokenized)):
        tokenized[row_index] = map(lambda x: snowball_st.stem(x), tokenized[row_index])

    #val = 'nan'
    #while val in tokenized:
    #   tokenized.remove(val)


    return tokenized, english_mask


def getwordcounts(unique_identifier, stemmedresult, colname):
    '''
    The function creates a bag-of-words dataframe where columns are words and
    cells for each row represent an instance of that stemmed word in use. (a count of words)
    Expects as input a list of words used throughout all rows and a stemmed result of words for
    each row. Returns Dataframe of stemmed wordcounts.
        cidlist = a list of company unique identifiers as identified by LinkedIn
        stemmedresult = a list of lists that includes stemmed and split words for each record. This should
        be able to intake any stemmed result coming from the cleantext function.

    :rtype : wordcount: a dummied dataframe containing word percentages for words used in a text blob.
    '''
    # replicate unique identifier cid n number of times depending on now many words exist in each company record

    #unique_identifier=list(features['cid'])
    #colname='cspecialties'

    print "Dense wordcount starting for {}".format(colname)

    #put string na's where there is a blank
    for row in range(len(unique_identifier)):
        if stemmedresult[row] == []:
            stemmedresult[row] = ['na']

    wordreadycompanies = []
    i = 0
    for row in unique_identifier:
        wordreadycompanies.append([i] * len(stemmedresult[i]))
        i += 1
    print "Dense wordcount complete for {}".format(colname)

    # flatten both the cid list of depth-2 and the stemmedresult (list of words)

    wordreadycompanies = list(chain(*wordreadycompanies))
    stemmedresult = list(chain(*stemmedresult))

    d = {'cid' : wordreadycompanies, 'word': stemmedresult}
    wordcount = pd.DataFrame(data=d)

    # add column for count of words so it can be used in a pivot table. create a pivot dataframe with words as columns
    # and wordcounts per company record as the cell values.
    print "Pivot Table Creation starting for {}".format(colname)
    wordcount['count'] = 1
    wordcountx = pd.pivot_table(wordcount, index='cid', columns=['word'], values='count', aggfunc=lambda x: np.sum(x),
                                fill_value=0.0)
    print "Pivot Table Creation Complete for {}".format(colname)


    #alter the word counts to be the proportion of word use in a text blob out of the total number of words.
    #print "Start filling NA's in word columns for {}".format(colname)
    #wordcountx.apply(lambda x: x/np.sum(x, axis = 1))
    #for i in range(wordcountx.shape[1]):
    #    wordcountx.iloc[:, i] = wordcountx.iloc[:, i] / wordcountx.sum(axis=1).astype(float)
    #wordcountx.fillna(0.0, inplace=True)
    #print "Completed filling NA's in word columns for {}".format(colname)

    wordcountx.columns = colname + '-' + wordcountx.columns

    return wordcountx


def buildDummy(col):
    '''
	This function takes in a single column, deals with missing values, and returns a DataFrame
	with dummy columns
	'''
    print "Started to dummy {}".format(col)
    try:
        catcounts = col.groupby(col).count()
        #At this point I'm going to do a really naive imputation. What ever has the most.
        newcategory = catcounts[max(catcounts) == catcounts].index.tolist()
        col[pd.isnull(col)] = newcategory[0]
        dummied = pd.get_dummies(col)
    except:
        print "The Column is all NaNs"
        dummied = pd.DataFrame(col)

    return dummied


def consolidateTypeDummy(dummied_col):
    '''
	This function takes in a single column, deals with missing values, and returns a DataFrame
	with dummy columns
	'''
    final_dummied = pd.DataFrame()
    final_dummied['Privately Held'] = dummied_col['Privately Held'] + dummied_col['Partnership'] + \
                                      dummied_col['Self Employed'] + dummied_col['Self Owned']
    final_dummied['Government Agency'] = dummied_col['Government Agency']
    final_dummied['Educational or Non Profit'] = dummied_col['Educational'] + dummied_col['Non Profit']
    final_dummied['Public Company'] = dummied_col['Public Company']

    return final_dummied


def buildEmployeeCount(col):
    '''
    This function casts employee count column to float and imputes NAs as == 1001.0
    '''
    def checkmyself(y):
        if y == "myself only":
            y = "1"
        return y

    #impute all "myself only" answers as 1
    col_with_ones = map(checkmyself, col)

    finalemployeecount = [0] * len(col_with_ones)
    for j in range(len(col_with_ones)):
        count = re.match('^\d+', str(col_with_ones[j]))
        try:
            finalemployeecount[j] = count.group(0)
        except:
            finalemployeecount[j] = 1001.0

    finalemployeecount = [float(i) for i in finalemployeecount]
    finalemployeecount = pd.DataFrame(finalemployeecount)

    def build_cats(row):
        if row < 50:
            y = 'Very Small (Between 15 and 50 Employees)'
        elif 50 <= row < 200:
            y = 'Small (Between 50 and 200 Employees)'
        elif 200 <= row < 1000:
            y = 'Medium (Between 200 and 1000 Employees)'
        elif 1000 <= row < 5000:
            y = 'Large (Between 1000 and 500 Employees)'
        else:
            y = 'Massive (Over 5000 Employees)'
        return y

    result = map(lambda x: build_cats(x), finalemployeecount.values)
    result = pd.DataFrame(result)
    result.columns = ['cemployeecount']
    return result.iloc[:,0]


def buildFoundedYear(col):
    """
    This function deals with founded year.
    """
    nonna_cols = col[pd.notnull(col)].apply(lambda x: re.sub(',','',x)).astype('float')
    mean = nonna_cols.mean()
    col[pd.isnull(col)] = mean
    col = col.astype('str')
    final_cols = col.apply(lambda x: re.sub(',','',x)).astype('float')
    final_cols = float(dt.datetime.now().year) - final_cols
    final_cols = pd.DataFrame(final_cols)


    def build_cats(row):
        if row < 3.0:
            y = 'Infancy (1-3 years)'
        elif 3 <= row < 10:
            y = 'New (3-10 years)'
        elif 10 <= row < 30:
            y = 'Established (10-30 years)'
        elif 30 <= row < 50:
            y = 'Mature (30-50 years)'
        else:
            y = 'Foundational (Over 50 years)'
        return y

    result = map(lambda x: build_cats(x),final_cols.values)

    result = pd.DataFrame(result)
    result.columns = ['cage']
    return result.iloc[:,0]


def getJobs(filename):
    """
    Returns a jobs dataframe so each row is a single company instead of having to
    repeat the company for each position.
    :param filename: The filename of the jobs data csv from the linkedIn API
    :return: a dataframe called jobs containing the following column definitions
                jcompanyid =    The unique identifier for a company
                jtitle =        A list of job titles for each position for a company
                jdescription =  The concatenated descriptions for each positon a company
                has posted.
    """
    jobs = pd.read_csv(filename)
    jobs = jobs[pd.notnull(jobs)]
    jobs.rename(columns={'jcompanyid':'cid'}, inplace=True)
    jobs.cid[pd.isnull(jobs['cid'])] = 1
    remove_comma_cid = jobs['cid'].astype('str')
    remove_comma_cid = remove_comma_cid.apply(lambda x: re.sub(',','',x))
    jobs['cid'] = remove_comma_cid.apply(lambda x: re.sub('\..+','',x))
    jobs['cid'].astype('int')

    #might have to come back to this REGEX below as it is not working 100%
    seniority_stopwords = "(/s)?( consult|analys(t|is)|manage(r|ment)|vp|director|" \
                          "sr|jr|senior|chief|lead|supervisor|deputy|" \
                          "executive|assistant|chief|vice(/s)|intern|paid/sintern|" \
                          "associate|solution|region|sector|country|national|" \
                          "branch|mgr|service|region|expert|/d+ )(,|/s)?"

    seniority_removed = []
    jobs.jtitle[pd.isnull(jobs.jtitle)] = ''
    jobs['jtitle'] = jobs['jtitle'].astype('str')
    jobs['jdescription'] = jobs['jdescription'].astype('str')
    for row in jobs.index:
        jobs['jtitle'][row] = jobs['jtitle'][row].lower()
        seniority_removed.append(re.sub(seniority_stopwords,'', jobs['jtitle'][row]))
    jobs['jtitle'] = seniority_removed
    print "Seniority Stopwords have been removed from Job Titles (jtitle)."
        # combine all of the job descriptions for each company into a single text blob.
    job_descriptions, job_title = [], []
    jobs.reset_index()
    print "Starting pooling together job descriptions"
    for some_cid in jobs['cid']:
        job_descriptions.append(jobs.jdescription[jobs['cid'] == some_cid].sum())
        job_title.append(list(jobs.jtitle[jobs['cid'] == some_cid]))
    jobs['jtitle'] = job_title
    jobs['jdescription'] = job_descriptions
    jobs = jobs.groupby(jobs['cid']).first()
    print "Ending pooling together job descriptions"

    return jobs


def build_titles(col):
    """
    This function performs the nltk clean functions for job titles one row
    at a time by calling cleantext.  It then combines all the titles per
    company into a single bag of words per row (company).  The list of lists
    is returned.

    :rtype : list of lists.  where each sub-element is a list of job title words.
    :param col: The column 'jtitles' from the jobs dataframe
    """

    print "Job Titles starting to run through cleantext"
    clean_column = cleanflattext(col,'jtitle')
    print "Job Titles have been successfully run through cleantext"
    final = clean_column[0]

    return final


def buildIndustry(col):

    #load a pickle that includes my similarity matrix resulting from the industry taxonomy.
    ind_sim_file = 'data/ind_sim.pickle'
    with open(ind_sim_file, 'rb') as indsim:
        ind_sim = cPickle.load(indsim)

    #cast the col series as a dataframe otherwise the shape will be one dimensional and it won't work.
    col_as_dataframe = pd.DataFrame(col)
    col_as_dataframe['id'] = range(len(col_as_dataframe))

    #merge the raw industry column with the indsim similarity matrix. The outside merge it to get every row
    #back into the same order.
    x = col_as_dataframe.merge(ind_sim, how='left', on='cindustry', sort=False).fillna(0)
    x = x.sort('id')
    x = x.reset_index()
    x = x.drop('id',axis = 1)

    return x


########################
## CAPSTONE FUNCTIONS ##
########################

def munge_jobs(filename, keepcols):
    jobs = getJobs(filename)
    final_jobs = pd.DataFrame()
    titles = build_titles(jobs['jtitle'])
    fname_out = '{}-{}-{}-{}.pickle'.format('data/initialmunge/jtitle', dt.datetime.now().year, dt.datetime.now().month,
                                                dt.datetime.now().day)
    with open(fname_out, 'wb') as fout:
        cPickle.dump(titles, fout, -1)
    print "Success: jtitle Pickle Created!"

    #The two statements below 1. stems the job descriptions and 2. builds
    #the dummied frame for the job descriptions
    stemmed_job_description = cleanflattext(framecol=jobs['jdescription'], colname='jdescription')
    print "Job Descriptions (jdescription) have been cleaned and stemmed."
    fname_out = '{}-{}-{}-{}.pickle'.format('data/initialmunge/jdescription', dt.datetime.now().year, dt.datetime.now().month,
                                                dt.datetime.now().day)
    with open(fname_out, 'wb') as fout:
        cPickle.dump(stemmed_job_description[0], fout, -1)
    print "Success: jdescription Pickle Created!"

    #create the jtitle column so I have a way to see where the link is pose dimension reduction.
    final_jobs['jtitle'] = jobs['jtitle']
    final_jobs['cid'] = jobs['cid'].values
    print "JOBS COMPLETE"

    return final_jobs


def munge_companies(filename):
    """

    """
    features = pd.read_csv(filename)
    mungedData = pd.DataFrame()
    colnames = ['cdescription', 'ctype', 'cemployeecount', 'cfoundedyear',
                'cfollowers', 'cindustry', 'cstatus', 'cspecialties']
    newcol = pd.DataFrame()
    # the if/else block below deals specially with running munging for the 'cspecialties' column
    # as it has to go through some preprocessing steps.
    for i in colnames:
        if i == 'cstatus':
            newcol = buildDummy(features[i])
        if i == 'ctype':
            tempcol = buildDummy(features[i])
            newcol = consolidateTypeDummy(tempcol)
        if i == 'cindustry':
            newcol = buildIndustry(features[i])
        elif i == 'cfoundedyear':
            tempcol = buildFoundedYear(features[i])
            newcol = buildDummy(tempcol)
        elif i == 'cfollowers':
            newcol = pd.DataFrame(features[i])
        elif i == 'cemployeecount':
            tempcol = buildEmployeeCount(features[i])
            newcol = buildDummy(tempcol)
        elif i == 'cspecialties':
            features[i][pd.isnull(features[i])] = 'na'
            flat_specialties = ['o'] * len(features[i])
            #the loop below joins the 2nd level lists into single elements via a loop.
            # It uses the j element to access rows
            for row in range(len(features)):
                flat_specialties[row] = "".join(features.cspecialties[row])
            stemmedresult, keepcols = cleanflattext(framecol=flat_specialties, colname = i)
            fname_out = '{}-{}-{}-{}.pickle'.format('data/initialmunge/' + str(i), dt.datetime.now().year, dt.datetime.now().month,
                                                dt.datetime.now().day)
            with open(fname_out, 'wb') as fout:
                cPickle.dump(stemmedresult, fout, -1)
            print "Success: cspecialties Pickle Created!"
        if i == 'cdescription':
            features['cdescription'][pd.isnull(features['cdescription'])] = 'na'
            cdescstemmedresult, keepcols = cleanflattext(framecol=features['cdescription'], colname = 'cdescription')

            #pare down the features dataframe to match the length after I cut out some rows in the cleanflattext function.
            #this is so features can be appropriately used in other column munging rather than having mismatching sizes.
            features = features.loc[keepcols]
            #reindex the whole dataframe as now indexes are missing.
            features = features.reset_index()

            fname_out = '{}-{}-{}-{}.pickle'.format('data/initialmunge/' + str(i), dt.datetime.now().year, dt.datetime.now().month,
                                                dt.datetime.now().day)
            with open(fname_out, 'wb') as fout:
                cPickle.dump(cdescstemmedresult, fout, -1)
            print "Success: cdescription Pickle Created!"

        try:
            for col in newcol.columns:
                mungedData[col] = newcol[col].values
        except:
            pass

        print 'Success for column named {}'.format(i)

    print 'Success in combining all columns on the Company DataFrame!'
    mungedData['cname'] = features['cname'].values
    mungedData['cid'] = features['cid'].values
    print "COMPANIES COMPLETE"

    return mungedData, keepcols


def munge():
    """

    :param response:
    :return:

    """
    #get the most recent linkedin pull of data for the jobs and the companies.
company_files = []
for file in os.listdir('data/features/')[1:]:
    company_files.append(os.path.abspath('data/features/' + file))

    job_files = []
    for file in os.listdir('data/jobs/')[1:]:
        job_files.append(os.path.abspath('data/jobs/' + file))

    job_filename = max(job_files, key = os.path.getctime)
    company_filename = max(company_files, key = os.path.getctime)

    companies, keepcols = munge_companies(filename = company_filename)
    jobs_result = munge_jobs(filename = job_filename, keepcols = keepcols)
    jobs_result['cid'] = jobs_result['cid'].astype('int')
    job_cid = list(jobs_result.cid)

    jobcidfile = open('data/initialmunge/{}-{}-{}-{}.csv'.format('job-cid',
     dt.datetime.now().year, dt.datetime.now().month,
     dt.datetime.now().day), 'wb')
    wr = csv.writer(jobcidfile, quoting=csv.QUOTE_ALL)
    wr.writerow(job_cid)

    response_frame = pd.merge(companies, jobs_result, how='left', on='cid')
    print 'Joined Companies and Jobs DataFrames'
    #re-order columns for easier use. Putting cid, cname and ctitle on the left hand side.
    column_order = response_frame.columns.tolist()[-3:]
    column_order.extend(response_frame.columns.tolist()[:-3])
    response_frame = response_frame.loc[:,column_order]
    print column_order
    #save everything to a csv.
    response_frame.to_csv('data/initialmunge/{}-{}-{}-{}.csv'.format('cquant',
     dt.datetime.now().year, dt.datetime.now().month,
     dt.datetime.now().day), index=False, encoding='utf-8')
    print 'data/initialmunge/{}-{}-{}-{}.csv'.format('cquant', dt.datetime.now().year, dt.datetime.now().month,
                                                dt.datetime.now().day)

    print 'ALL MUNGING COMPLETE!'

