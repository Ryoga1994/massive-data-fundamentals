#!/usr/lib/env python35

# for more information about matplotlib, see:
# http://matplotlib.org/users/pyplot_tutorial.html

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import re

if __name__=="__main__":
    # # Create some fake data
    # run1 = [1,2,3] # data for file 1
    # run2 = [2,4,5] # data for file 2
    # run3 = [1,10,2] # data for file 3
    #
    # xpoints = [1,2,3] # index of file

    # input data
    i = 0 # count to ignore comments
    lis = []

    with open('q2_results.txt','r') as f:
        import json
        for line in f:
            if(line.startswith('{')):
                lis.append(json.loads(line))

    data = pd.DataFrame(lis)

    # extract index of input file
    pat = re.compile(r's3://gu-anly502/A1/quazyilx(.+?).txt')

    for i in range(len(data)):
        data.ix[i,'file_index'] = re.findall(pat,data.ix[i,'input:'])[0]

    run1 = data.ix[data['file_index']=='1',].sort('nodes',ascending=True)
    run2 = data.ix[data['file_index'] == '2',].sort('nodes',ascending=True)
    run3 = data.ix[data['file_index']=='3',].sort('nodes',ascending=True)


    run1_line = plt.plot(run1['nodes'],run1['seconds'], "r--", label="run1")
    run2_line = plt.plot(run2['nodes'],run2['seconds'], "bs-", label="run2")
    run3_line = plt.plot(run3['nodes'],run3['seconds'], "g^-", label="run3")
    plt.title("A comparision of three runs")
    plt.xlabel('Number of nodes')
    plt.ylabel('time')
    plt.legend()

    # This would display locally:
    # plt.show()
    # This will save the figure as a png and as a PDF:
    plt.savefig("q2_plot.png")
    plt.savefig("q2_plot.pdf")
