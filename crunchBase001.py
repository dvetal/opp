#!/usr/bin/env python
# -*- coding: utf-8 -*-
 
from urllib2 import urlopen
import json
import pandas as pd
import datetime as dt
import time
import sys

###############
## FUNCTIONS ##
###############

def getOrgContent(orgPath,orgsData,userKey,index):
    '''
    Retrieves company detailed information based upon the orgPath, orgData, userKey, and index

    Input Variables:
        orgPath =               (list)(strings) the list of strings for org paths. i.e. the 
                                HTML path needed to retrieve each org detail.
        orgsData =              (dict) the simple organization list including metadata. This is 
                                simply usedto build and HTML call.
        userKey =               (string) Your userkey for use of hte API provided by CrunchBase.
        index =                 (int) the index number from the orgPath list that we want to use.
    Return: 
    SingleOrgData =             (dict) JSON dict of an individual org's data 
    '''
    crunchBaseSingleOrg = urlopen(orgsData['metadata']['api_path_prefix'] + 
        orgPath[index] + '?user_key=' + userKey)
    SingleOrgResponse = crunchBaseSingleOrg.read()
    SingleOrgData = json.loads(SingleOrgResponse)
    return SingleOrgData


def callback(orgData, cName, cType, cDescription):
    '''
    Based upon detailed organization data, this function munges the JSON data into lists of
    data I actually want, i.e. Name, Type, and Description.  This callback function is a 
    placeholder incased I want to build this API interaction out as an Asynchronous call.

        Input Variables:
            orgData =               (list)(strings) the list of strings for org paths.
                                    i.e. the HTML path needed to retrieve each org detail.
            cName =                 (list)(strings) Company Names
            cType =                 (list)(strings) Company Type
            cDescription =          (list)(strings) Company Description 
    '''
    try:
        cName.append(orgData['data']['properties']['name'])
        cType.append(orgData['data']['properties']['primary_role'])
        cDescription.append(orgData['data']['properties']['short_description'])
        return cName, cType, cDescription

    except:
        cName.append('NA')
        cType.append('NA')
        cDescription.append('NA')
        return cName, cType, cDescription


def getCrunchBase(key,callLimitPerMinute,startPage,filePathName):
    '''
    Retrieves company name, company type, and company description from CrunchBase for all 
    companies starting at a particular page number.  Pages have 1000 companies per page. Returns
    a datafram with company name, company type, and company description.

        Input Variables:
            key =                   (string) Your userkey for use of hte API provided by 
                                    CrunchBase
            callLimitPerMinute =    (int) The Crunchbased specified maximum number of API
                                    calls you can make per minute. (currently 45)
            startPage =             (int) Starting page of companies you would like to 
                                    start at. Currently there are ~280 pages; making about 300K
                                    records.
            filePathName =          (string) The relative path and filename of the output file
                                    with extension name.
        Return:
            crunchBaseCompanies     (DataFrame) Includes company Name, Type, and Description.
    '''
    #Initialize empty lists for the result
    companyName = list()
    companyType = list()
    companyDescription = list()

    #below retrieves the first page of companies including metadata.  The metadata, namely the total
    #number of pages is used to bound the for loop below.
    OrgsLoadDict = dict()
    crunchBaseOrgs = urlopen("http://api.crunchbase.com/v/2/organizations?user_key=" + 
        key)
    OrgsResponse = crunchBaseOrgs.read()
    OrgsLoadDict = json.loads(OrgsResponse)

    #Outer loop below loops through the pages in range beginning with the startPage.
    for page in range(startPage,OrgsLoadDict['data']['paging']['number_of_pages']):
        print 'Starting page # %d of %d' % (page, OrgsLoadDict['data']['paging']['number_of_pages'])
        OrgsData = list()
        crunchBaseOrgs = urlopen("http://api.crunchbase.com/v/2/organizations?user_key=" + 
            key + '&page=' + str(page))
        OrgsResponse = crunchBaseOrgs.read()
        OrgsData = json.loads(OrgsResponse)

        #the while loop below retrieves a list of Org Paths for the page fo companies from the data.
        #The companies paths will be used to concatenate together an API call specific to each company
        #where I can retrieve teh company description and detailed information.
        index = 0
        OrgPathList = list()
        while (index < len(OrgsData['data']['items'])):
            OrgPathList.append(OrgsData['data']['items'][index]['path'])
            index += 1

        #CrunchBase does have a cap on the number of calls you can make in a minute.  The code below 
        #goes through every record on an individual page.  Within the page, it cafely calls the API
        #bounded by the call limit and then waits until it can make another call.  Once it can, it
        #does.
        record = 0
        last_job = dt.datetime(2001,8,30,16,5,35,403829)
        while (record < len(OrgsData['data']['items'])):
            try:
                waitTime = 60 - (dt.datetime.now() - last_job).total_seconds()
                time.sleep(waitTime)
                print 'Wait Time is %.2f seconds' % waitTime
            except:
                last_job = dt.datetime.now()
                for j in range(1,callLimitPerMinute-1):
                    last_small_job = dt.datetime.now()
###FIX###           OrgData = getOrgContent(orgPath = OrgPathList, orgsData = OrgsData, 
#                       userKey = key, index = record)
                    callback(OrgData, cName = companyName, 
                        cType = companyType, 
                        cDescription = companyDescription)
                    record += 1
            print(record)    
        #Then I either start a new file with the information in it or I append to the existing
        #file for every page to limit loss in the cases where there is a drop of the API connection
        if page == startPage:
            pd.DataFrame.to_csv(crunchBaseCompanies, encoding = 'utf-8',path_or_buf = filePathName)
        else:
            with open(filePathName, 'a') as f:
                crunchBaseCompanies.to_csv(f,header=False)

        #Below I need to wait before I start a new page as my current loop just starts automatically
        #when it the next page starts.
        try:
                waitTime = 60 - (dt.datetime.now() - last_job).total_seconds()
                time.sleep(waitTime)
                print 'Wait Time is %.2f seconds' % waitTime
        except:
            continue

#########################
## CONSOLE INTERACTION ##
#########################

#Below I am simply setting up my ability to run this script in the console (bash,etc.)
def main(argv):
    if len(argv) == 3:
        print crunchBaseCompanies(argv[0],argv[1],argv[2])
    else:
        print "Wrong number of arguments"

##############
## WORKFLOW ##
##############

mykey = 'c27b5d55bb03e208ab92b5466c1453aa'
getCrunchBase(key = mykey,callLimitPerMinute = 40,startPage = 1,filePathName = 'data/crunchbasecomps.csv')
