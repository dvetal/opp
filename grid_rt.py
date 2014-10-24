

import re
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import nltk
import cPickle
from gensim import corpora, models
import topic_analysis
import csv
import datetime as dt

def cleanpersontext(text):
    """
    This function expects a dataframe column or a list of unstructured text.

    For Example:
    ['She's a man eater.','The dog jumped over the brown log.','I would like to have a brown dog?']
    """
    print "REGEX Starting for {}".format('Resume')
    #make sure all rows are strings.
    #decode the entire string into utf-8 for each row.
    text = text.decode('utf-8')
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
    person_topic_frame = combined.lda(num_topics = num_topics)

    return person_topic_frame


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


def processForm():

    resume_file = str(raw_input("Please enter the location of your resume file (txt): "))

    quantoutput = [0] * 13

    #get user to import their resume as a txt file. Not sure how this will work yet with the file importing.

    quantoutput[1] = 9999999999
    #get the preferred title for a person
    quantoutput[2] = str(raw_input("Please provide a title that fits you: ")).lower()
    if len(quantoutput[2]) > 70:
        print "Error! Only 70 characters allowed!"
    #put everything in the title as lowercase so nothing gets revoved inadvertantly as being a proper noun in my REGEX
    #script.
    #get data on whether or not a person liked a certain company while working for them
    all_positions = []
    position_flag = 'y'
    while position_flag == 'y':
        company_position = ['empty'] * 2
        position_flag = raw_input("Add a company? (Y/N) ")
        position_flag = position_flag.lower()
        if position_flag != 'y':
            break

        while True:
            company_position[0] = str(raw_input("Company Name: "))
            if len(company_position[0]) <= 70:
                break
            else:
                print "Error! Only 70 characters allowed!"

        while True:
            try:
                company_position[1] = int(raw_input("Did you like the company? (1-5, 5 = Absolutely, 1 = Not at all)"))
                if 0 < company_position[1] < 5:
                    break
                else:
                    print "You must input a number between 1 and 5"
            except:
                print 'That\'s not a number'

        all_positions.append(company_position)


    #get the optimal companysize
    while True:
        try:
            quantoutput[11] = float(raw_input('Optimal company Size by employee 1-20000: '))
            if 0 < quantoutput[11] < 20001:
                break
            else:
                print 'Out of Range'
        except:
            print 'That\'s not a number'

    while True:
        try:
            quantoutput[12] = float(raw_input('Optimal company age in years: 1 to 200'))
            if 0 < quantoutput[12] < 201:
                break
            else:
                print 'Out of Range'
        except:
            print 'That\'s not a number'



    #get the preferred company type
    while True:
        try:
            ctype = int(raw_input('What is the optimal company type for you? 1. Educational, 2. Government Agency,'
                                    '3. Non-Profit, 4. Partnership, 5. Privately Held, 6. Public Company'))
            if 0 < ctype < 7:
                if ctype == 1:
                    quantoutput[3] = 1
                if ctype == 2:
                    quantoutput[4] = 1
                if ctype == 3:
                    quantoutput[5] = 1
                if ctype == 4:
                    quantoutput[6] = 1
                if ctype == 5:
                    quantoutput[7] = 1
                break
            else:
                print 'Out of Range'
        except:
            print 'That\'s not a number.'

    return quantoutput, all_positions, resume_file


def fitToMaster(raw_text, config, column_name):
    '''
    Fit a new document to the existing corpus of companies/jobs sets of information. Return a list with the similarity
    values to each topic.
    :param raw_text: The raw text from a resume other text field.
    :param column_name: The column name in the form that will retrieve the proper saved model from the larger set of
    data in this case either jtitle or jdesc will work
    :return: A list of topic similarities for a specific column
    '''
    #basic natural language munging
    clean_text = cleanpersontext(raw_text)

    #load the dictionary from the master set of companies from linked in
    master_dictionary = corpora.Dictionary.load('data/dictionaries/' + column_name + '.dict')
    vector = master_dictionary.doc2bow(clean_text)
    topic_vector = []
    #load the fitted model from the master set of companies from linked in
    lda = models.utils.SaveLoad.load('data/dictionaries/' + column_name)
    #get vector of topics as a list of tuples
    topic_vector = lda[vector]

    #get the 2nd element of each tuple and put into a list.
    result_list = []
    for topic in range(config['jobs'][column_name]):
        for tuple in range(len(topic_vector)):
            if topic_vector[tuple][0] == topic:
                result_list.append(topic_vector[tuple][1])
                break
            elif tuple == len(topic_vector)-1:
                result_list.append(0.0)

    return result_list


def buildFullPersonFeatures(config):
    '''
    This function includes asking a person for their resume and a few questions about their preferences on companytype.
    It then takes all the data and build a list that can easily be concatenated with the dataframe that has all of the
    companies in it.
    :param config: The configuration file used for determining the number of topics for each field that is text.
    :return: A list including all of the data for a person
    '''

    #load the config file

    #execute the questions to the user. return quantitative data, position history, and the bar resume filename.
    resume_file = 'data/resume/DonaldVetal_Resume.txt'
    person_quant = [0, 9999999999, 'data scientist', 0, 0, 0, 0, 1, 0, 0, 0, 300.0, 6.0]
    x =  open(resume_file, 'r')
    resume_text = x.read()

    jobs_field_list = config['jobs'].keys()

    #add both the raw resume text and the person's defined ideal title.  Using the dict allows me to deal with scenarios
    #where I am doing something different than topic analysis on one of the fields.
    raw_job_text = {}
    if config['jobs']['jdesc'] > 0:
        raw_job_text['jdesc'] = resume_text
    if config['jobs']['jtitle'] > 0:
        raw_job_text['jtitle'] = person_quant[2]


    #Go through text analysis against the larger corpus and determine
    # topic similarity and build a single list out of it.
    i = 0
    job_text_result = []
    for field in jobs_field_list:
        print field
        job_text_result.extend(fitToMaster(raw_text = raw_job_text.values()[i], config=config, column_name=field))
        i += 1

    #Add data for company description and specialties which should be all zeros because that data does not exist for
    #a person.
    comp_desc = [0.0] * config['companies']['cdesc']
    comp_spec = [0.0] * config['companies']['cspec']

    person_result = person_quant + job_text_result + comp_desc + comp_spec

    #append another column to the end to account for the jobflag.
    person_result.append(0)

    #write the person result to a csv file for use in machine learning.
    csvfile = open('data/finalFrame/{}-{}-{}-{}.csv'.format('personready',
         dt.datetime.now().year, dt.datetime.now().month,
         dt.datetime.now().day),'wb')

    wr = csv.writer(csvfile)
    wr.writerow(person_result)
    print "COMPLETE RT Pipeline: CSV created in finalFrame"

