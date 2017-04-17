#!/usr/bin/python34
#
# Template to compute the number of distinct hostname
#

from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

line = '2016-07-01T00:01:01,1,,192.168.10.1,1798,2,,96.120.104.177,9739,3,,68.87.130.233,11766,4,ae-53-0-ar01.capitolhghts.md.bad.comcast.net,68.86.204.217,11203,5,be-33657-cr02.ashburn.va.ibone.comcast.net,68.86.90.57,14575,6,he-0-2-0-0-ar01-d.westchester.pa.bo.comcast.net,68.86.94.226,17923,7,bu-101-ur21-d.westchester.pa.bo.comcast.net,68.85.137.213,16070,8,,68.87.29.59,16761'

class DistinctHostname(MRJob):

    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        # Your code goes here
        pat = re.compile('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+,[0-9]+,[0-9]+,([^,]+?),')
        for hostname in pat.findall(line):
            yield hostname,1

    def reducer(self, word, counts):
        # Your code goes here
        _sum = sum(1 for _ in counts)
        yield word,str(_sum)


if __name__ == '__main__':
    DistinctHostname.run()