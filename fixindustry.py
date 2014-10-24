import pandas as pd
import os
import datetime as dt

file_list = os.listdir('data/industry')
file_list = file_list[1:-2]
target = pd.read_csv('data/features/combfeatures-2014-10-20.csv')
print target[target['cid'] == 17357]
print target[target['cid'] == 7547]
print target[target['cid'] == 74570]
print target[target['cid'] == 162830]
print pd.isnull(target['cindustry']).sum()


for some_file in file_list:
    source = pd.read_csv('data/industry/' + some_file)
    for id in source['cid']:
        print id
        target['cindustry'][target['cid'] == id] = source['cindustry'][source['cid'] == id].values[0]

print target[target['cid'] == 17357]
print target[target['cid'] == 7547]
print target[target['cid'] == 74570]
print target[target['cid'] == 162830]
print pd.isnull(target['cindustry']).sum()

target.to_csv(
        '/Users/donaldvetal/PublicProjects/oppfinder/data/features/{}-{}-{}-{}.csv'.format('features', dt.datetime.now().year,
                                                                                  dt.datetime.now().month,
                                                                                  dt.datetime.now().day), index=False,
        encoding='utf-8')