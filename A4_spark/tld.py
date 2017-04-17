#!/usr/bin/env python3.4
#
# Insert your tld.py function below.

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

import re

top1m = sc.textFile("/top-1m.csv").cache()

# extract domain name

# line = '1,google.comrozblog.com'

def extract_domain(line):
    return line.split('.')[-1]

tlds = top1m.map(lambda line: [extract_domain(line),1])

tlds.cache()
tlds_and_counts = tlds.countByKey()

counts_and_tlds = [(count,domain) for (domain,count) in tlds_and_counts.items()]

counts_and_tlds.sort(reverse=True)

# Store the results of counts_and_tlds in a file called q3_counts.txt

open("q3_counts.txt","w").write(str(counts_and_tlds[0:50]))