from linkedin import linkedin
import datetime as dt
import csv
import pandas as pd


##############
## OATH 1.0 ##
##############


# Company:

# WMayer.com.br

# Application Name:

# Find Jobs

# API Key:

# 775l5jz4au73p7

# Secret Key:

# AylXUKqiM8Nep5et

# OAuth User Token:

# 8e3abf1f-a603-40ee-bd44-7235f0202cf1

# OAuth User Secret:

# 35846244-d6fb-47d1-ab9d-be58f64bb2c4





CONSUMER_KEY = '775l5jz4au73p7'
CONSUMER_SECRET = 'AylXUKqiM8Nep5et'
USER_TOKEN = 'abc2b7eb-d40a-4384-b3fd-97abf85d911e'
USER_SECRET = '8ff0395b-f5f8-4476-a422-31a6aa774101'
RETURN_URL = 'https://linked.wmayer.com.br/redirect2'

authentication = linkedin.LinkedInDeveloperAuthentication(CONSUMER_KEY, CONSUMER_SECRET,
                                                          USER_TOKEN, USER_SECRET,
                                                          RETURN_URL, linkedin.PERMISSIONS.enums.values())

application = linkedin.LinkedInApplication(authentication)

###############
## FUNCTIONS ##
###############


def dprint(msg, time=None):
    if not time:
        time = dt.datetime.now()
    print '{} - {}'.format(time.strftime('%Y-%m-%d %H:%M:%S'), msg)


def getRawCompanyDetails(companyids):
    """
    This call to the LinkedIn API retrieves detailed company information.  It reads from a csv file and
    starts at the next index that it didn't reach the last time.
    """
    #make the API call.
    companydetaildicts = []
    companydetaildicts.append(application.get_companies(company_ids=companyids,
                                                        selectors=['name', 'id', 'industries']))
    return companydetaildicts


def getCompanyDetails(rawcompanydetails):
    '''
	This function re-packages detailed company information into a dataframe from JSON
	'''
    import pandas as pd

    cid = []
    cindustry = []
    cname = []

    for singlecompany in range(len(rawcompanydetails[0]['values'])):
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

    companyfeatures = pd.DataFrame()
    companyfeatures['cindustry'] = cindustry
    companyfeatures['cname'] = cname
    companyfeatures['cid'] = cid

    companyfeatures.to_csv(
        '{}-{}-{}-{}.csv'.format('BUDDYfeatures', dt.datetime.now().year,
            dt.datetime.now().month,
            dt.datetime.now().day), index=False,
            encoding='utf-8')


##############
## WORKFLOW ##
##############
delta = 500
FileIGaveYou = ''

dprint('Begining process')
test = pd.read_csv('data/industry/Linkedin/combfeatures-2014-9-30.csv')
x = test.cid[test['cindustry'].isnull()]
with open('data/industry/Linkedin/lastPosition.txt') as f:
    last_id = int(f.readline())
companyids = list(x[last_id:(last_id+delta)])

dprint('Raw details')
rawJSON = getRawCompanyDetails(companyids)

dprint('Company details')
getCompanyDetails(rawJSON)

with open('data/industry/Linkedin/lastPosition.txt', 'w') as f:
    f.write(str(last_id + delta))
dprint('End of process')

sample = pd.read_csv('data/industry/BUDDYfeatures-2014-10-10.csv')