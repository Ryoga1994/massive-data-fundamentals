#!/usr/bin/env python34

#
# Placeholder
#

import heapq
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

TOPN = 10

class HitCountTopN(MRJob):

    def mapper(self, _, line):
        pat = re.compile('"GET\s(.+?)\sHTTP')
        if len(re.findall(pat,line))>0:
            yield re.findall(pat,line)[0], 1

    def reducer(self, word, counts):
        yield word, sum(counts)

    def topN_mapper(self, word, count):
        yield "Top" + str(TOPN), (count, word)

    def topN_reducer(self, _, countsAndWords):
        for countAndWord in heapq.nlargest(TOPN, countsAndWords):
            yield _, countAndWord

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
            reducer=self.reducer),
            MRStep(mapper=self.topN_mapper,
            reducer=self.topN_reducer)]

if __name__ == "__main__":
    HitCountTopN.run()

