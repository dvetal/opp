import grid_dt
import grid_rt
import clustertext
import matplotlib.pyplot as plt
import math
import numpy as np
#def plot_cluster_spread(self):
#    pass

#    x = Vetal.distance['cluster'].groupby(Vetal.distance['cluster']).count().sort(inplace = False, ascending = False)

#    plt.plot(x)
#    plt.show()


id_filename = 'data'
cluster_perf, cluster_skew = [], []

config = {
    'jobs': {
        'jdesc': 100,
        'jtitle': 20
    },
    'companies': {
        'cdesc': 100,
        'cspec': 30
    }
}

grid_dt.executeDTMungePipeline(id_filename, config=config, pullAPIData = False, munge_new = False)
grid_rt.buildFullPersonFeatures(config=config)

c, p, j = clustertext.loadFinalData()
j = [[1123, 4], [5349578, 5]]
result = clustertext.Career(company = c, person = p, job_history = j)
result.run_cluster_kmeans(clusters = 80)
result.get_distance()
cluster_perf.append(result.cluster_performance)

cluster_skew.append(result.distance['cluster'].groupby(result.distance['cluster']).
                    count().sort(inplace = False, ascending = False))

plt.plot(param_list, cluster_perf)
plt.yticks()
plt.xlabel('Number of Clusters')
plt.ylabel('Performance: (Inner Cluster / Inter Cluster)')
plt.title('# of Clusters vs. Cluster Performance')
plt.ylim((0.0,0.5))
plt.gca().invert_yaxis()
plt.show()
plt.savefig('data/plots/clusterperfvstopics.png')

skew_data = cluster_skew

for element_index in range(len(cluster_skew)):
    cluster_skew[element_index] = map(lambda m: math.log(m+1.0), cluster_skew[element_index])
    plt.bar(cluster_skew[element_index])

plt.legend(param_list, loc ='upper right')
plt.title('Cluster vs. Log(# of Records - 1)')
plt.xlabel('Cluster Number')
plt.ylabel('# of Records')
plt.show()