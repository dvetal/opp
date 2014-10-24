import re
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import nltk
import cPickle
import datetime as dt
from gensim import corpora, models
import topic_analysis
import pandas as pd

def cleanpersontext(text):
    """
    This function expects a dataframe column or a list of unstructured text.

    For Example:
    ['She's a man eater.','The dog jumped over the brown log.','I would like to have a brown dog?']
    """
    print "REGEX Starting for {}".format('Resume')
    #make sure all rows are strings.
    try:
        #decode the entire string into utf-8 for each row.
        text = text.decode('utf-8')
    except:
        text = text

    #Take out the section with hobbies
    text = re.sub('/s[H|h]obb[y|i].*',' ', text)
    #remove words that are found in the middle of a sentence and are capitalized. Remove the \r's and \n's found throughout.
    #remove digits.
    text = re.sub('\s[A-Z]\w+|\r|\n|\d+',' ', text)
    #try to remove basic web addresses starting with www.
    text = re.sub('w{3}.\w+.\\b',' ', text)
    #remove all non-alphanumeric characters no matter the length
    text = re.sub('\W+',' ', text)
    #make all words lowercase
    text = text.lower()
    #get rid of words that are under 3 letters long.
    text = re.sub('(\s|^)(\S{0,2}(\s|$))+',' ', text)
    #get rid of words that have three letters in a row
    text = re.sub('([a-z])\1{2}\b',' ', text)

    print "REGEX Complete for {}".format('Resume')
    #english mask generates a list of indexes (rows) that are companies that are speaking in english.

    #import the nltk stopwords list
    my_stopwords = stopwords.words('english') + ['com','senior','vice','president','analyst','senior','junior','director',
                                                 'manager','supervisor','chief','deputy','ceo','coo','cp','cfo','cio',
                                                 'company']

    #create a string that is ready to add to a regex expression that includes all oc
    print "Stopword removal Starting for {}".format('Resume')
    preregex = '|'.join(my_stopwords)
    stopwords_regex = '(\s|^)((' + preregex + ')(\s|$))+'
    success = re.sub(stopwords_regex,' ',text)
    print "Stopword removal Complete for {}".format('Resume')


    snowball_st = SnowballStemmer("english")

    print "Tokenization Starting for {}".format('Resume')
    tokenized = nltk.word_tokenize(success)
    print "Tokenization Complete for {}".format('Resume')

    for row_index in range(len(tokenized)):
        tokenized[row_index] = snowball_st.stem(tokenized[row_index])

    return tokenized


def buildJDescSpace(jpickle, resume_filename, num_topics, column):

    #open up and read a resume as a single string.
    with open(jpickle, 'rb') as jdescriptions:
        jdesc = cPickle.load(jdescriptions)

    #get the resume text
    resume_text = open(resume_filename, 'r').read()

    #clean the resume text through NLTK and regex
    clean_resume = cleanpersontext(resume_text)

    #insert the resume text int othe first position (0) so it can be found later.
    jdesc.insert(0, clean_resume)

    def removeall_replace(x, l):
        t = [y for y in l if y != x]
        del l[:]
        l.extend(t)

    for row_index in range(len(jdesc)):
        removeall_replace('nan',jdesc[row_index])
        removeall_replace('nannan',jdesc[row_index])

    #run the topic_analysos and return a dataframe as we want it.
    combined = topic_analysis.Corpus(jdesc,column)
    combined.create_corpus()
    combined.create_tfidf()
    person_topic_frame = combined.lsi(num_topics = num_topics)

    return person_topic_frame


def buildIndustrySpace(industry):

    #load a pickle that includes my similarity matrix resulting from the industry taxonomy.
    ind_sim_file = 'data/ind_sim.pickle'
    with open(ind_sim_file, 'rb') as indsim:
        ind_sim = cPickle.load(indsim)

    #find the matching industry in the industry similarity matrix
    result = list(ind_sim[ind_sim['cindustry'] == industry].values[0])[:-1]

    return result


def buildJTitleSpace(jtitlespickle, persontitle, num_topics, column):

    #open up and read a resume as a single string.
    with open(jtitlespickle, 'rb') as jtitles:
        jtitles = cPickle.load(jtitles)

    #clean the resume text through NLTK and regex
    clean_title = cleanpersontext(persontitle)

    #insert the resume text int othe first position (0) so it can be found later.
    jtitles.insert(0, clean_title)

    def removeall_replace(x, l):
        t = [y for y in l if y != x]
        del l[:]
        l.extend(t)

    for row_index in range(len(jtitles)):
        removeall_replace('notitle',jtitles[row_index])
        removeall_replace('nan',jtitles[row_index])
        removeall_replace('nannan',jtitles[row_index])

    #run the topic_analysos and return a dataframe as we want it.
    combined = topic_analysis.Corpus(jtitles,column)
    combined.create_corpus()
    combined.create_tfidf()
    person_title_frame = combined.lda(num_topics = num_topics)

    return person_title_frame


def processForm(person, history):

    resume_file = person['resume']
    industry = person['industry']

    quantoutput = [0] * 17

    #get user to import their resume as a txt file. Not sure how this will work yet with the file importing.

    quantoutput[1] = 9999999999
    #get the preferred title for a person
    quantoutput[2] = person['title']

    all_positions = []
    for comp_index in range(len(history)):
        all_positions.append([ history[comp_index]['company'], history[comp_index]['rating'] ])

    # Get the preferred company size
    while True:
        try:
            if 0 < person['size'] < 6:
                if person['size'] == 1:
                    quantoutput[11] = 1
                elif person['size'] == 2:
                    quantoutput[10] = 1
                elif person['size'] == 3:
                    quantoutput[9] = 1
                elif person['size'] == 4:
                    quantoutput[7] = 1
                else:
                    quantoutput[8] = 1
                break
            else:
                print 'Out of Range'
        except:
            print 'That\'s not a number.'


    while True:
        try:
            if 0 < person['age'] < 6:
                if person['age'] == 1:
                    quantoutput[14] = 1
                elif person['age'] == 2:
                    quantoutput[16] = 1
                elif person['age'] == 3:
                    quantoutput[12] = 1
                elif person['age'] == 4:
                    quantoutput[15] = 1
                else:
                    quantoutput[13] = 1
                break
            else:
                print 'Out of Range'
        except:
            print 'That\'s not a number.'


    #get the preferred company type
    while True:
        try:
            if 0 < person['type'] < 5:
                if person['type'] == 1:
                    quantoutput[3] = 1
                if person['type'] == 2:
                    quantoutput[4] = 1
                if person['type'] == 3:
                    quantoutput[5] = 1
                else:
                    quantoutput[6] = 1
                break
            else:
                print 'Out of Range'
        except:
            print 'That\'s not a number.'

    return quantoutput, all_positions, resume_file, industry


def fitToMaster(clean_text, config, column_name):
    '''
    Fit a new document to the existing corpus of companies/jobs sets of information. Return a list with the similarity
    values to each topic.
    :param raw_text: The raw text from a resume other text field.
    :param column_name: The column name in the form that will retrieve the proper saved model from the larger set of
    data in this case either jtitle or jdesc will work
    :return: A list of topic similarities for a specific column
    '''

    #load the dictionary from the master set of companies from linked in
    master_dictionary = corpora.Dictionary.load('data/dictionaries/' + column_name + '.dict')
    vector = master_dictionary.doc2bow(clean_text)
    #load the fitted model from the master set of companies from LinkedIn
    lda = models.utils.SaveLoad.load('data/dictionaries/' + column_name)
    #get vector of topics as a list of tuples
    topic_vector = lda[vector]

    #get the 2nd element of each tuple and put into a list.
    result_list = []
    maj_list = ['jobs','companies']
    for maj in maj_list:
        try:
            for topic in range(config[maj][column_name]):
                try:
                    for tuple in range(len(topic_vector)):
                        if topic_vector[tuple][0] == topic:
                            result_list.append(topic_vector[tuple][1])
                            break
                        elif tuple == len(topic_vector) - 1:
                            result_list.append(0.0)
                except:
                    continue
        except:
            continue

    return result_list


def buildFullPersonFeatures(configfile, person, history):
    '''
    This function includes asking a person for their resume and a few questions about their preferences on companytype.
    It then takes all the data and build a list that can easily be concatenated with the dataframe that has all of the
    companies in it.
    :param configfile: The configuration file used for determining the number of topics for each field that is text.
    :return: A list including all of the data for a person
    '''
    config = eval(open(configfile).read())

    #execute the questions to the user. return quantitative data, position history, and the bar resume filename.
    person_quant, all_positions, resume_file, industry = processForm(person=person, history=history)

    x =  open(resume_file, 'r')
    resume_text = x.read()

    #save job history to a file of this naming pattern
    fname_out = '{}-{}-{}-{}.pickle'.format('data/finalFrame/jobhistory', dt.datetime.now().year, dt.datetime.now().month,
                                                dt.datetime.now().day)

    #dump the job history into a pickle for later use in ranking.
    with open(fname_out, 'wb') as fout:
        cPickle.dump(all_positions, fout, -1)
    print "Success: Job history Pickle Created!"

    jobs_field_list = config['jobs'].keys() + ['cdesc']

    #add both the raw resume text and the person's defined ideal title.  Using the dict allows me to deal with scenarios
    #where I am doing something different than topic analysis on one of the fields.
    raw_job_text = {}
    if config['jobs']['jdesc'] > 0:
        raw_job_text['jdesc'] = cleanpersontext(resume_text)
    if config['jobs']['jtitle'] > 0:
        raw_job_text['jtitle'] = cleanpersontext(person_quant[2])
    if config['companies']['cdesc'] > 0:
        raw_job_text['cdesc'] = raw_job_text['jdesc']

    #add columns for industries by matching the industry column values to the industry that the user put in.
    industry_columns = buildIndustrySpace(industry)


    #Go through text analysis against the larger corpus and determine
    # topic similarity and build a single list out of it.
    i = 0
    job_text_result = []
    for field in jobs_field_list:
        job_text_result.append(fitToMaster(clean_text = raw_job_text.values()[i], config=config, column_name=field))
        i += 1


    #Add data for company description and specialties which should be all zeros because that data does not exist for
    #a person.
    comp_spec = [0.0] * config['companies']['cspec']

    person_result = person_quant + industry_columns + job_text_result[2] + comp_spec + job_text_result[0] + job_text_result[1]

    #append another column to the end to account for the jobflag.
    person_result.append(0)

    p = pd.DataFrame(person_result)

    #write the person result to a csv file for use in machine learning.
    person_file = open('data/finalFrame/{}-{}-{}-{}.csv'.format('personready',
         dt.datetime.now().year, dt.datetime.now().month,
         dt.datetime.now().day), 'w')

    p.to_csv(person_file, index=False, encoding='utf-8')
    print 'Success! Data input correctly'
    return person_result

