from linkedin import linkedin
import sys
import datetime as dt
import csv
import pandas as pd
import os


# #############
# # OATH 1.0 ##
##############

CONSUMER_KEY = '77l2naesxip31g'
CONSUMER_SECRET = 'M6m5T7qJ5klQNzv4'
USER_TOKEN = '49027919-884e-41d5-a01c-4978ec7b765a'
USER_SECRET = '732deacd-17c4-45c0-b12e-072f48b07f39'
RETURN_URL = 'http://donvetal.com:8000/'

authentication = linkedin.LinkedInDeveloperAuthentication(CONSUMER_KEY, CONSUMER_SECRET,
                                                          USER_TOKEN, USER_SECRET,
                                                          RETURN_URL, linkedin.PERMISSIONS.enums.values())

application = linkedin.LinkedInApplication(authentication)

###############
## FUNCTIONS ##
###############

def geticodes(filename):
    """
	This function retrieves a list of meaningful industry codes to use when searching for companies to add to the training data.
    :rtype : object
	"""

    industryCodeLocation = filename
    with open(industryCodeLocation) as industryCodesFile:
        rawlist = list(csv.reader(industryCodesFile))[1:]

    industrycodes = list()
    for i in range(len(rawlist)):
        if rawlist[i][0] == "":
            continue
        else:
            industrycodes.append(rawlist[i][0])
    return industrycodes


def getCompanySample(searchlimit, industrycodes, pagestart):
    """
	This function retrieves a list of company ids split fairly evenly across industries.
	"""
    count = 20
    companyrawresult = []
    callsleft = int(searchlimit)

    while callsleft > 0:
        # POSSIBLE IMPROVEMENT: pull industry companies based upon the proportion of total companies that
        # they represent
        j = 0
        while j < len(industrycodes):
            callsleft = callsleft - 1
            if callsleft == 0:
                break
            try:
                companyrawresult.append(application.search_company(selectors=[{'companies': ['id']}],
                                                                   params={'facet': 'industry,' + str(industrycodes[j]),
                                                                           'count': count, 'start': pagestart}))
                print "Success! {}".format(j)
            except:
                print "Failure at {}".format(j)
                industrycodes.pop(j)
                continue
            j += 1
            if callsleft == 0:
                break
        print "Loop Complete: {} calls left.".format(callsleft)
        pagestart += count

    companyid = []
    for l in range(count):
        for m in range(len(companyrawresult)):
            try:
                companyid.append(companyrawresult[m]['companies']['values'][l]['id'])
            except:
                continue

    csvfile = open('/Users/donaldvetal/PublicProjects/oppfinder/data/{}-{}-{}-{}.csv'.format('companyids', dt.datetime.now().year,
                                                                                  dt.datetime.now().month,
                                                                                  dt.datetime.now().day), 'wb')
    wr = csv.writer(csvfile)
    wr.writerow(companyid)

    return pagestart


def getRawCompanyDetails(id_filename):
    """
    This call to the LinkedIn API retrieves detailed company information.  It reads from a csv file and
    starts at the next index that it didn't reach the last time.
    """
    ### LOAD CSV of company id's ####
    companyids = []
    csvfile = open(id_filename, 'rb')
    companyreader = csv.reader(csvfile)
    for row in companyreader:
        companyids.append(row)
    companyidlist = companyids[0]

    #read in the current starting point on the index
    companyindexfile = open('data/companyidstart.txt', 'r')
    start_index = int(companyindexfile.readline())
    companyindexfile.close()

    #get only 500 companyids
    try:
        companyidlist = companyidlist[start_index:start_index + 500]
    except:
        companyidlist = companyidlist[start_index:]

    #make the API call.
    companydetaildicts = []
    companydetaildicts.append(application.get_companies(company_ids=companyidlist,
                                                        selectors=['name', 'id', 'universal-name', 'company-type',
                                                                   'industries', 'status', 'square=logo-url',
                                                                   'employee-count-range', 'specialties', 'description',
                                                                   'founded-year', 'end-year', 'num-followers',
                                                                   'twitter-id']))
    #open the file that keeps the last id we reached and write the number into the file of where we are
    companyindexfile = open('data/companyidstart.txt', 'w')
    companyindexfile.write(str(start_index + 500))
    companyindexfile.close()

    return companydetaildicts


def getCompanyDetails(rawcompanydetails):
    '''
	This function re-packages detailed company information into a dataframe from JSON
	'''
    import pandas as pd

    ctype = [];
    cdescription = [];
    cemployeecount = [];
    cfoundedyear = [];
    cid = [];
    cindustry = []
    cname = [];
    cfollowers = [];
    cspecialties = [];
    clogo = [];
    cstatus = [];
    ctwitter = []
    cuniname = []

    for singlecompany in range(len(rawcompanydetails[0]['values'])):
        try:
            ctype.append(rawcompanydetails[0]['values'][int(singlecompany)]['companyType']['name'])
        except:
            ctype.append('')
        try:
            cdescription.append(rawcompanydetails[0]['values'][int(singlecompany)]['description'])
        except:
            cdescription.append('')
        try:
            cemployeecount.append(rawcompanydetails[0]['values'][int(singlecompany)]['employeeCountRange']['name'])
        except:
            cemployeecount.append('')
        try:
            cfoundedyear.append(rawcompanydetails[0]['values'][int(singlecompany)]['foundedYear'])
        except:
            cfoundedyear.append('')
        try:
            cid.append(rawcompanydetails[0]['values'][int(singlecompany)]['id'])
        except:
            cid.append('')
        try:
            cindustry.append(rawcompanydetails[0]['values'][int(singlecompany)]['industries']['values'][0]['name'])
        except:
            cindustry.append('')
        try:
            cname.append(rawcompanydetails[0]['values'][int(singlecompany)]['name'])
        except:
            cname.append('')
        try:
            cfollowers.append(rawcompanydetails[0]['values'][int(singlecompany)]['numFollowers'])
        except:
            cfollowers.append('')
        try:
            cspecialties.append(rawcompanydetails[0]['values'][int(singlecompany)]['specialties']['values'])
        except:
            cspecialties.append('')
        try:
            clogo.append(rawcompanydetails[0]['values'][int(singlecompany)]['squareLogoUrl'])
        except:
            clogo.append('')
        try:
            cstatus.append(rawcompanydetails[0]['values'][int(singlecompany)]['status']['name'])
        except:
            cstatus.append('')
        try:
            ctwitter.append(rawcompanydetails[0]['values'][int(singlecompany)]['twitterId'])
        except:
            ctwitter.append('')
        try:
            cuniname.append(rawcompanydetails[0]['values'][int(singlecompany)]['universalName'])
        except:
            cuniname.append('')

    companyfeatures = pd.DataFrame()
    companyfeatures['ctype'] = ctype
    companyfeatures['cdescription'] = cdescription
    companyfeatures['cemployeecount'] = cemployeecount
    companyfeatures['cfoundedyear'] = cfoundedyear
    companyfeatures['cindustry'] = cindustry
    companyfeatures['cfollowers'] = cfollowers
    companyfeatures['cspecialties'] = cspecialties
    companyfeatures['cstatus'] = cstatus
    companyfeatures['cuniname'] = cuniname
    companyfeatures['cname'] = cname
    companyfeatures['cid'] = cid
    companyfeatures.set_index(['cid'])

    companywidgets = pd.DataFrame()
    companywidgets['clogo'] = clogo
    companywidgets['ctwitter'] = ctwitter
    companywidgets['cid'] = cid
    companywidgets['cuniname'] = cuniname
    companywidgets['cname'] = cname
    companywidgets.set_index(['cid'])

    return companywidgets, companyfeatures


def getCompanyUpdates(id_filename):
    '''
	This function retrieves company update information.  Meaning any updates they have made on LinkedIn.  
	The number of most recent updates to retrieve is given by the 'count' variable. 
    :rtype : the location of the companyids filename. 'data/companyids.csv'
	'''

    companyids = []
    csvfile = open(id_filename, 'rb')
    companyreader = csv.reader(csvfile)
    for row in companyreader:
        companyids.append(row)
    companyidlist = companyids[0]

    companyindexfile = open('data/companyidstart.txt', 'r')
    start_index = int(companyindexfile.readline())
    companyindexfile.close()

    try:
        companyidlist = companyidlist[start_index:start_index + 500]
    except:
        companyidlist = companyidlist[start_index:]

    count = 10
    callsleft = 700
    companyupdateslist = []
    for compid in companyidlist:
        print compid
        try:
            companyupdateslist.append(application.get_company_updates(compid, params={'count': 10}))
            'Success in getting company update for {}'.format(compid)
            callsleft = callsleft - 1
            if callsleft == 0: break
        except:
            companyupdateslist.append('')
            if callsleft == 0: break
            continue

    jcompanyid = [];
    jdescription = [];
    jid = [];
    jtitle = []

    for company in range(len(companyupdateslist)):
        for updateindex in range(count):
            try:
                jcompanyid.append(companyupdateslist[company]['values'][updateindex]['updateContent']['company']['id'])
            except:
                jcompanyid.append('')
            try:
                jdescription.append(
                    companyupdateslist[company]['values'][updateindex]['updateContent']['companyJobUpdate']['job'][
                        'description'])
            except:
                jdescription.append('')
            try:
                jid.append(
                    companyupdateslist[company]['values'][updateindex]['updateContent']['companyJobUpdate']['job'][
                        'id'])
            except:
                jid.append('')
            try:
                jtitle.append(
                    companyupdateslist[company]['values'][updateindex]['updateContent']['companyJobUpdate']['job'][
                        'position']['title'])
            except:
                jtitle.append('')

    jobfeatures = pd.DataFrame()
    jobfeatures['jcompanyid'] = jcompanyid
    jobfeatures['jdescription'] = jdescription
    jobfeatures['jid'] = jid
    jobfeatures['jtitle'] = jtitle

    return jobfeatures


def getConnectionData():
    '''
	This function retrieves the companies that a user's connections works for.  Returns a dataframe with a single column of
	company ids.
	'''
    #### THIS IS TO RETRIEVE CONNECTIONS for the USER. As of now this is not setup for OATH 2.0 ###
    connectiondata = application.get_connections(selectors=['headline', 'first-name', 'positions'])

    concompanyid = [];
    contitle = [];
    for person in range(len(connectiondata['values'])):
        try:
            for position in range(len(connectiondata['values'][person]['positions']['values'])):
                try:
                    concompanyid.append(
                        connectiondata['values'][person]['positions']['values'][position]['company']['id'])
                except:
                    concompanyid.append('')
        except:
            concompanyid.append('')

    connections = pd.DataFrame()
    connections['cid'] = concompanyid

    return connections


def getcompanyids():
    """
	This function queries linked in for company new company ids that we need to use to call details for each company.
	"""
    industrycodesfile = 'data/icodes.csv'
    startpagefile = open('data/pagestart.txt', 'r')
    pagestart = startpagefile.readline()
    pagestart = int(pagestart)
    startpagefile.close()

    industry = geticodes(filename=industrycodesfile)
    pageend = getCompanySample(searchlimit=470, industrycodes=industry, pagestart=pagestart)

    startpagefile = open('data/pagestart.txt', 'w')
    startpagefile.write(str(pageend))
    startpagefile.close()


def getdetails(id_filename):
    """
	This function produces updated Dataframes for the base features of the companies (written to features),
	the widgets to use on the website (written to widgets), and jobs for each company (written to jobs).
	"""
    crawdetails = getRawCompanyDetails(id_filename=id_filename)
    widgets, features = getCompanyDetails(rawcompanydetails=crawdetails)
    jobs = getCompanyUpdates(id_filename=id_filename)

    widgets.to_csv(
        '/Users/donaldvetal/PublicProjects/oppfinder/data/widgets/{}-{}-{}-{}.csv'.format('widgets', dt.datetime.now().year,
                                                                                  dt.datetime.now().month,
                                                                                  dt.datetime.now().day), index=False,
        encoding='utf-8')
    features.to_csv(
        '/Users/donaldvetal/PublicProjects/oppfinder/data/features/{}-{}-{}-{}.csv'.format('features', dt.datetime.now().year,
                                                                                  dt.datetime.now().month,
                                                                                  dt.datetime.now().day), index=False,
        encoding='utf-8')
    jobs.to_csv(
        '/Users/donaldvetal/PublicProjects/oppfinder/data/jobs/{}-{}-{}-{}.csv'.format('jobs', dt.datetime.now().year,
                                                                                  dt.datetime.now().month,
                                                                                  dt.datetime.now().day), index=False,
        encoding='utf-8')


def combineData():
    """
    This function combines all of the raw data feeds we get daily into a single dataframe ready for nlmunge.
    Output includes three seperate dataframes as csv file in their respective directories. (jobs,features,widgets)
    """

    #Retrieve the list of current files in each directory for jobs, features, and widgets
    jobs = os.listdir('data/jobs')
    jobs.pop(0)
    features = os.listdir('data/features')
    features.pop(0)
    widgets = os.listdir('data/widgets')
    widgets.pop(0)

    #generate lists to be used to build filenames and access each directory seperately through a loop
    directories = [jobs, widgets, features]
    directory_names = ["jobs", "widgets", "features"]

    #the double loop below goes through each directory (jobs, widgets, features) and looks at each file and combines
    #those files that belong in the same directory into a single dataframe by adding ROWS.
    #TODO: This poops out wen I run dt_munge.py but it runs fin independently
    combined_frames_list = []
    combined_frames_index = 0
    for directory in directories:
        combined_frame = pd.DataFrame()
        for a_file in range(len(directory)):
            newdf = pd.read_csv('data/' + directory_names[combined_frames_index] + '/' + directory[a_file])
            combined_frame = pd.concat([newdf,combined_frame], ignore_index=True)
            combined_frame.drop_duplicates(inplace = True)
        combined_frames_list.append(combined_frame)
        combined_frames_index += 1

    #the loop below simply states if the correct number of columns were generated.  If not it prints an error. The
    #correct nubmer of columns are respectively 4,5, and 11.
    correct_column_number = [4,5,11]
    for frame_index in range(len(combined_frames_list)):
        if combined_frames_list[frame_index].shape[1] == correct_column_number[frame_index]:
            print "{} dataframe created CORRECTLY. There are currently {} rows. Check for correctness. ".\
                format(directory_names[frame_index],combined_frames_list[frame_index].shape[1],
                       combined_frames_list[frame_index].shape[0])
        else:
            print "{} dataframe created INCORRECTLY with {} columns".\
                format(directory_names[frame_index],
                       combined_frames_list[frame_index].shape[1])

    combined_frames_list[2] = combined_frames_list[2].iloc[:,2:]
    #The loop below builds a new file with the combined dataframe in each folder which is now ready for nlmunge.
    for frame_index in range(len(combined_frames_list)):
        combined_frames_list[frame_index].to_csv(
            '/Users/donaldvetal/PublicProjects/oppfinder/data/' + directory_names[frame_index] +
            '/{}-{}-{}-{}.csv'.format('comb' + directory_names[frame_index], dt.datetime.now().year, dt.datetime.now().month,
                                      dt.datetime.now().day), index=False, encoding='utf-8')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Usage: getdata'
    else:
        getdata()