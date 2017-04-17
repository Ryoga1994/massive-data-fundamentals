#!/usr/bin/python34


import matplotlib.pyplot as plt
import sys
import re
import pandas as pd

if __name__=="__main__":

    urls = []
    values = []

    pat = re.compile('\[(.+?),(.+?)\]')

    with open("q5_result.txt") as f:
        lines = f.readlines()

        for line in lines:
            num_hit, url = re.findall(pat, line)[0]

            urls.append(re.sub('"|\s', '', url))
            values.append(int(num_hit))

    plt.bar(range(len(urls)),values)
    plt.title("Top 10 hits")
    plt.xlabel('rank of url')
    plt.ylabel('number of hits')

    # This would display locally:
    # plt.show()
    # This will save the figure as a png and as a PDF:
    plt.savefig("q5_plot.png")
    plt.savefig("q5_plot.pdf")
