"""
The intention of this script is to build a reusable pipeline for clustering text intensive dataframes from csv files.
"""
import pandas as pd
import numpy as np
from sklearn import cluster
import cPickle
import datetime as dt
import sklearn.preprocessing as skpp
import sklearn.neighbors as neighbors
import os
import re
import rt_munge as rt

class Career():

    def __init__(self, company, person, job_history):
        self.company = company
        self.person = person
        self.job_history = job_history
        self.features = pd.concat([person, company], axis = 0)
        self.features.reset_index()

    def run_cluster_kmeans(self,clusters = 8, init = 'k-means++', n_init = 10, max_iter = 100):
        """
        This function conducts an Agglomerative Clustering Fit on a Career and saves the fit to a binary pickle file.
        :param TextFrame: A dataframe that was created with some dummied words from text as columns.
        :param method: either (single, complete, average, weighted, ward, centroid, median)
        :param metric:
        """
        self.num_clusters = clusters
        self.__normalize()

        normalized_array = np.array(self.company.iloc[:,2:-1])

        hyperparameters = cluster.KMeans(n_clusters = clusters, init = init, n_init = n_init, max_iter = max_iter)
        #print "Hyper-parameters have been set"
        fit = hyperparameters.fit(normalized_array)
        #print "Success: KMeans Clustering Complete. clusters = {} " \
        #      "init = {} n_init = {}, max_iter = {}".format(clusters, init, n_init, max_iter)

        fname_out = '{}.pickle'.format('data/models/fit')
        with open(fname_out, 'wb') as fout:
            cPickle.dump(fit, fout, -1)
        print "Success: Pickle Created!"
        self.fit = fit

    def run_cluster_agg(self, clusters = 2, affinity = 'euclidean', linkage = 'average'):
        """
        This function conducts an Agglomerative Clustering Fit on a Career and saves the fit to a binary pickle file.
        :param TextFrame: A dataframe that was created with some "dummied" words from text as columns.
        :param method: either (single, complete, average, weighted, ward, centroid, median)
        :param metric:
        """
        normalized = self.__normalize()
        normalized_array = np.array(normalized)

        hyperparameters = cluster.AgglomerativeClustering(n_clusters = clusters,
                                                          affinity = affinity, linkage = linkage)
        #print "Hyper-parameters have been set"
        fit = hyperparameters.fit(normalized_array)
        #print "Success: Agglomerative Clustering Complete. clusters = {} " \
        #      "affinity = {} linkage = {}".format(clusters, affinity, linkage)

        fname_out = '{}-{}.pickle'.format('data/models/agg', dt.datetime.now())
        with open(fname_out, 'wb') as fout:
            cPickle.dump(fit, fout, -1)
        #print "Success: Pickle Created!"
        self.fit = fit

    def get_distance(self, industry_config, phase, dist_metric = 'euclidean', w_cdesc=1.0, w_cspec=1.0, w_cindustry=1.0,
        w_jdesc=1.0, w_jtitle=1.0, w_ctype=1.0, w_age=1.0, w_csize=1.0):
        '''
        This function calculates the distances between each company and the person/user. Later used for ranking
        '''
        #first normalize the person data
        if phase == 1:
            self.__normalize_person()
        #then apply weights
        self.__apply_weights(industry_config, w_cdesc, w_cspec, w_cindustry, w_jdesc, w_jtitle, w_ctype, w_age, w_csize)

        # X is the entire set of records minus the row we don't need
        X = np.array(self.features.iloc[:,2:len(self.features.columns)-1])
        # Y is the users data minus the rows
        Y = np.array(self.features.iloc[self.features[self.features['cid'] == 9999999999].index, 2:len(self.features.columns)-1])
        #the line below simply initializes the distance calculation method
        dist = neighbors.DistanceMetric.get_metric(dist_metric)
        #get distances
        distances = dist.pairwise(X,Y)

        #build a dataframe with the data I want in it
        distance_frame = pd.DataFrame()
        distance_frame['cid'] = self.features['cid']
        distance_frame['cname'] = self.features['cname']
        distance_frame['distance'] = distances

        #Load the design-time fit from a binary
        with open('data/models/fit.pickle', 'rb') as bit:
            self.fit = cPickle.load(bit)

        try:
            clusters = np.insert(self.fit.labels_, obj=0,values=0, axis=0)
            distance_frame['cluster'] = clusters
        except:
            pass

        self.distance = distance_frame

        #added this back in so I can normalize distances between companies with jobs and those without
        self.distance['jobflag'] = self.features['jobflag']

        ##### BUILD CLUSTER METRIC #################################
        #The next 4 lines build a metric to measure the performance of clustering
        #inter_cluster_distances = dist.pairwise(self.fit.cluster_centers_)
        #denominator = inter_cluster_distances.sum() / 2
        #numerator = self.fit.inertia_

        #The smaller the better because this compares the numerator as the sum of the distances on average between the
        # center and the points for each cluster.  While the denominator is the sum of the inter-cluster distances.
        #self.cluster_performance = numerator / denominator
        ############################################################
        self.distance = distance_frame



    def run_rank(self, hist_weight=1, dist_weight=1, top = 5):
        '''
        Calculates the top N best matches for a user and returns a dataframe with the company information.
        '''
        #Based upon the cid's of a person's work history, determine what cluster they belong to.
        history_cluster = []
        for index in range(len(self.job_history)):
            try:
                history_cluster.append(self.distance.cluster[self.distance['cid'] == self.job_history[index][0]].get_values()[0])
                self.history_clusters = history_cluster
            except:
                continue

        #Based upon a person's sentiment towards past companies score that company
        history_score = [0] * len(self.distance)
        for index in range(len(history_cluster)):
            try:
                cluster_match_flag = np.array((self.distance['cluster'] == history_cluster[index]).astype(float))
                score_multiplier = self.job_history[index][1] - 3
                history_score += cluster_match_flag * score_multiplier
            except:
                break

        #get the scores for the distances a company is away from a person.
        raw_distance_score = np.array(map(lambda x: 1/x, self.distance.distance))

        #add both the history and distance scores together to get a final score.
        #if the user did not enter a company this will error.  the try except takes care of avoind that.
        try:
            total_score = dist_weight * raw_distance_score + hist_weight * history_score
        except:
            total_score = dist_weight * raw_distance_score

        #add total score to the distance dataframe.  So you get a score for each record.
        self.distance['total_score'] = total_score

        #if a record matches perfectly it will be NA.  this makes sure they get great scores!
        self.distance['total_score'][pd.isnull(self.distance['total_score'])] = 9999999999.99
        self.rank = self.distance.sort('total_score', ascending = False)[1:top+1]
        linkedin = map(lambda x: 'https://www.linkedin.com/company/' + str(x), self.rank['cid'])
        self.rank['LinkedIn'] = linkedin

    def normalize_score_on_job(self, top = 5):
        #the next three / four lines simply normalize between companies with and companies without jobs.
        with_job_mean =  self.distance.total_score[ self.distance['jobflag'] == 1].mean()
        without_job_mean =  self.distance.total_score[ self.distance['jobflag'] == 0].mean()

        self.distance.total_score[self.distance['jobflag'] == 0] = self.distance.total_score[ self.distance['jobflag'] == 0] * \
                                                                   with_job_mean / without_job_mean

        self.rank = self.distance.sort('total_score', ascending = False)[1:top+1]



    def user_evaluation(self):
        pass


    def __apply_weights(self, industry_config, w_cdesc=1.0, w_cspec=1.0, w_cindustry=1.0, w_jdesc=1.0, w_jtitle=1.0,
                        w_ctype=1.0, w_age=1.0, w_csize=1.0):


        #apply weights to the company quant data
        cindustry_list = industry_config
        ctype_list = ['Privately Held','Government Agency','Educational or Non Profit', 'Public Company']
        csize_list = ['Large (Between 1000 and 500 Employees)','Massive (Over 5000 Employees)',
                      'Medium (Between 200 and 1000 Employees)','Small (Between 50 and 200 Employees)',
                      'Very Small (Between 15 and 50 Employees)']
        cage = ['Established (10-30 years)','Foundational (Over 50 years)','Infancy (1-3 years)','Mature (30-50 years)',
                'New (3-10 years)']

        quant_list = [w_ctype, w_csize, w_age, w_cindustry]
        column_sets = [ctype_list, csize_list, cage, cindustry_list]

        for index in range(len(column_sets)):
            try:
                weight = quant_list[index]
                self.features[column_sets[index]] = weight * self.features[column_sets[index]]
            except:
                continue

        text_weight_dict = {'cdesc': w_cdesc, 'cspec': w_cspec, 'jdesc': w_jdesc, 'jtitle': w_jtitle}
        if text_weight_dict['cdesc'] > 0 or text_weight_dict['cspec'] > 0 or text_weight_dict['jdesc'] > 0 or text_weight_dict['jtitle'] > 0:
            for key in text_weight_dict:
                matched_cols = [col for col in self.features.columns if key in col]
                self.features[matched_cols] = self.features[matched_cols] * float(text_weight_dict[key])
        print 'hello'

    def __normalize(self):
        array_for_normalization = np.array(self.company.iloc[:, 2:])
        normalized = skpp.normalize(array_for_normalization, norm='l2', axis=0, copy=True)
        self.company.iloc[:,2:] = normalized

    def __normalize_person(self):
        array_for_normalization = np.array(self.features.iloc[:, 2:].astype(float))
        normalized_person = skpp.normalize(array_for_normalization, norm='l2', axis=0, copy=True)
        self.features.iloc[:, 2:] = normalized_person


def loadFinalPersonData(persondata):
    '''
    Load the final cleaned information from linkedIn and the user.  Combine the person data with the company data into
    a single dataframe.
    :return: Combined dataframe with company and person information. A list of tuples for a person's job history;
    includes ratings for firms.
    '''

    #look at the finalFrame folder and identify the most recent files for each the company frame, the person list, and
    #the job information (i.e. how well they liked their company.
    item_list = ['company', 'person', 'job']
    file_dict = {}
    found_files = []
    for element  in item_list:
        for file_index in range(len(os.listdir('data/finalFrame/'))):
            if re.findall(element,os.listdir('data/finalFrame/')[file_index]):
                #find files that have a name that includes the item name
                found_files.append(os.path.abspath('data/finalFrame/' + os.listdir('data/finalFrame/')[file_index]))
                #out of those found file select the one that is most recently updated.
        found_file = max(found_files, key = os.path.getctime)
        #reset found_files
        found_files = []
        file_dict[element] = found_file

    #get company data
    company = pd.read_csv(file_dict['company'])

    #get job history data
    with open(file_dict['job'], 'rb') as bit:
        job_history = cPickle.load(bit)

    company.drop('index', 1, inplace=True)
    person = np.array(persondata).reshape(1, -1)
    person = pd.DataFrame(person, columns=company.columns)
    person.drop('jtitle', 1, inplace=True)
    person.iloc[0,2:-1] = person.iloc[0,2:-1].astype(float)
    person['cid'] = person['cid'].astype(int)

    company.drop('jtitle', 1, inplace=True)
    company.iloc[:, 2:len(company.columns) - 1] = company.iloc[:, 2:len(company.columns) - 1].astype(float)
    company.iloc[:, len(company.columns) - 1] = company.iloc[:, len(company.columns) - 1].astype(float)
    company.iloc[:, 1] = company.iloc[:, 1].astype(int)
    company.reset_index()
    print('Success! Data loaded from everywhere correctly')
    return company, person, job_history


##############
## CAPSTONE ##
##############

def run_master_ranker(person, history):
    person = rt.buildFullPersonFeatures(configfile='topic_config.py', person = person, history = history)
    c, p, j = loadFinalPersonData(persondata=person)
    j = [[1123, 4], [5349578, 5]]

    #c phase 1 is just the basic company information not including the company description, specialties, job decriptions and title
    c_phase_2 = c.iloc[:,116:]
    p_phase_2 = p.iloc[:,116:]
    c_phase_1 = c.iloc[:,0:116]
    p_phase_1 = p.iloc[:,0:116]
    c_phase_1['jobflag'] = c.iloc[:,-1:]
    p_phase_1['jobflag'] = p.iloc[:,-1:]

    #get the cid and cname back into the phase two dataframes so I can filter.
    c_phase_2_final = pd.concat([c_phase_1.iloc[:,0:2], c_phase_2], axis=1)
    p_phase_2_final = pd.concat([p_phase_1.iloc[:,0:2], p_phase_2], axis=1)

    person_phase_1 = Career(company=c_phase_1, person=p_phase_1, job_history=j)
    #person_phase_1.run_cluster_kmeans(clusters = 40)
    config = eval(open('industry_config.py').read())
    person_phase_1.get_distance(industry_config=config, phase=1, w_cdesc=0, w_cspec=0, w_cindustry=5, w_jdesc=0,
                           w_jtitle=0, w_ctype=1, w_age=2, w_csize=2)
    person_phase_1.run_rank(hist_weight=0, dist_weight=1, top = 100)

    #Merge the 500 cids to cut down round 2 to only the top 500 companies.
    c_phase_2_final = c_phase_2_final[c_phase_2_final['cid'].isin(person_phase_1.rank['cid'])]

    person_phase_2 = Career(company=c_phase_2_final, person=p_phase_2_final, job_history=j)
    person_phase_2.run_cluster_kmeans(clusters = 10)
    person_phase_2.get_distance(industry_config=config, phase=2, w_cdesc=1, w_cspec=0, w_cindustry=0, w_jdesc=3,
                           w_jtitle=1, w_ctype=0, w_age=0, w_csize=0)
    top = 10
    person_phase_2.run_rank(hist_weight=0, dist_weight=1, top = top)
    #person_phase_2.normalize_score_on_job(top = top)
    person_phase_2.rank['jobflag'][person_phase_2.rank['jobflag'] == 1] = "Position Available!"
    person_phase_2.rank['jobflag'][person_phase_2.rank['jobflag'] == 0] = ""
    result = person_phase_2.rank[['cname','LinkedIn','jobflag']]

    return result

#some_person = {
#            'title': 'Data Scientist',
#            'age': 2,
#            'industry': 'Internet',
#           'size': 2,
#            'resume': 'data/resume/DonaldVetal_Resume.txt',
#            'type': 1
#        }
#ADRIANO.WEIHMAYER.ALMEIDA.txt
#Resume_Amy_Vetal.txt

#some_history = [{'company': 1123, 'rating': 4}, {'company': 5349578, 'rating': 5}]
#result = run_master_ranker(person= some_person, history = some_history)
#print 'done'





