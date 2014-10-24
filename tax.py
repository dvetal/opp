import csv
import numpy as np
import cPickle
import pandas as pd

with open('data/icodes.csv') as codes:
    icodes = list(csv.reader(codes))

minor_cats = map(lambda x: x[2], icodes)
major_cats = map(lambda x: x[1], icodes)

sim_array = np.zeros((len(minor_cats),len(minor_cats)))

for i in range(len(sim_array)):
    for j in range(len(sim_array)):
        if major_cats[i] == major_cats[j]:
            sim_array[i][j] = 0.5
        if minor_cats[i] == minor_cats[j]:
            sim_array[i][j] = 1
sim_df = pd.DataFrame(sim_array, columns = minor_cats)
sim_df['cindustry'] = minor_cats
fname_out = 'data/ind_sim.pickle'
with open(fname_out, 'wb') as fout:
    cPickle.dump(sim_df, fout, -1)
print "Success: jtitle Pickle Created!"