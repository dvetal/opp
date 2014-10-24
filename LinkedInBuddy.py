from linkedin import linkedin
import datetime as dt
import csv
import pandas as pd


##############
## OATH 1.0 ##
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
        '/Users/donaldvetal/PublicProjects/oppfinder/data/features/{}-{}-{}-{}.csv'.format('BUDDYfeatures', dt.datetime.now().year,
                                                                                  dt.datetime.now().month,
                                                                                  dt.datetime.now().day), index=False,
        encoding='utf-8')

###############
## WORKFLOW  ##
###############

FileIGaveYou = ''
test = pd.read_csv('/Users/donaldvetal/PublicProjects/oppfinder/data/features/combfeatures-2014-9-30.csv')
x = test.cid[test['cindustry'].isnull()]
companyids = list(x[0:2])
rawJSON = getRawCompanyDetails(companyids)
getCompanyDetails(rawJSON)
