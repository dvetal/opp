"""
This script performs topic analysis on a set of files.
"""
import performTopicAnalysis as ta
import pandas as pd
import datetime as dt
import linkedIn001
import nlmunge2
import os
import re


def getNewFiles(folder,matchlist):
    '''
    Get the newest needed files from a folder of my choosing.
    :param folder:
    :param matchlist:
    :return:
    '''
    newestfilelist = []
    for item in matchlist:
        matching = []
        for file_index in range(len(os.listdir('data/' + folder + '/'))):
            if re.findall(item,os.listdir('data/' + folder + '/')[file_index]):
                matching.append(os.path.abspath('data/' + folder + '/' + os.listdir('data/' + folder + '/')[file_index]))
        newestfilelist.append(max(matching, key = os.path.getctime))

    return newestfilelist


def executeDTMungePipeline(id_filename, config, pullAPIData = False, munge_new = False):

    if pullAPIData == True:
        linkedIn001.getdetails(id_filename)
        linkedIn001.combineData()
    if munge_new == True:
        nlmunge2.munge()

    #run topic analysis for the files I want. Seperated by company and jobs because there are different number of
    # records.  This pulls from a config file that I have defined that keeps the information so it can be re-used to
    #built the proper amount of columns for the runtime information needed for an individual person.

    ta.execute(folder = 'initialmunge', matchlist = config['jobs'].keys(), num_topics=config['jobs'].values())
    ta.execute(folder = 'initialmunge', matchlist = config['companies'].keys(), num_topics=config['companies'].values())


    #grab the files in the post topic analysis that are most recent and build dataframes out of them
    post_topic_analysis = getNewFiles(folder = 'posttopicanalysis', matchlist = ['cdesc','jdesc'])
    company = pd.read_csv(post_topic_analysis[0])
    job = pd.read_csv(post_topic_analysis[1])

    #grab the files that DID NOT go through topic analysis i.e. quantitative stuff and CID data
    ##  and build dataframes out of them
    quant_jobcid = getNewFiles(folder = 'initialmunge', matchlist = ['job-cid','cquant'])
    jobcid = list(pd.read_csv(quant_jobcid[0]))
    company_quant = pd.read_csv(quant_jobcid[1])

    #This simply cuts up the quant dataframe until I get back the industry columns.
    leftquant = company_quant.iloc[:,:13]
    rightquant = company_quant.iloc[:,1116:125]
    company_all = pd.concat([leftquant,rightquant,company], axis =1)

    #Add a job flag so I can later use the flag to normalize distances between companies with jobs and companies without
    ## jobs
    job['jobflag'] = 1
    #add back in the CIDs associated with each job.  CID are the company unique identifiers
    job['cid'] = map(int, jobcid)

    #merge the company data with the job data.  Remove all NA's ( usually generated by companies without jobs.
    mlready = pd.merge(company_all, job, how='left', on='cid')
    mlready = mlready.groupby(mlready['cid']).first().fillna(0.0)
    mlready.rename(columns={'0': 'csize'}, inplace=True)

    #save the file to a csv
    mlready.to_csv('data/finalFrame/{}-{}-{}-{}.csv'.format('companyready',
         dt.datetime.now().year, dt.datetime.now().month,
         dt.datetime.now().day), index=False, encoding='utf-8')

    print "COMPLETE DT Pipeline: CSV created in finalFrame"

##############
## WORKFLOW ##
##############

