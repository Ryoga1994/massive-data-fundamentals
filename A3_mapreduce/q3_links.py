#!/usr/bin/python34
#
# Template to compute the number of distinct IP addresses
#

from mrjob.job import MRJob
import re
import statistics
from mrjob.protocol import TextProtocol


line = '2016-07-01T00:01:01,1,,192.168.10.1,1798,2,,96.120.104.177,9739,3,,68.87.130.233,11766,4,ae-53-0-ar01.capitolhghts.md.bad.comcast.net,68.86.204.217,11203,5,be-33657-cr02.ashburn.va.ibone.comcast.net,68.86.90.57,14575,6,he-0-2-0-0-ar01-d.westchester.pa.bo.comcast.net,68.86.94.226,17923,7,bu-101-ur21-d.westchester.pa.bo.comcast.net,68.85.137.213,16070,8,,68.87.29.59,16761'

class DistinctLinks(MRJob):

    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        # Your code goes here
        pat = re.compile('([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+),([0-9]+),*')
        pairs = pat.findall(line)
        for i in range(len(pairs)-1):
            yield '%s->%s'%(pairs[i][0],pairs[i+1][0]),int(pairs[i+1][1])-int(pairs[i][1]) # link, time

    def reducer(self, link, values):
        # Your code goes here
        c = 0 # count

        lis_time = []

        for v in values:
            c += 1
            lis_time.append(int(v))

        if c > 1:
            _stdev = statistics.pstdev(lis_time)
            # yield link, (c,_stdev)
            # print('%s\t%d\t%f'%(link,c,_stdev))
            yield link, "{}\t{}".format(c,_stdev)
        else:
            yield link, "{}\t{}".format(c,'--')


if __name__ == '__main__':
    DistinctLinks.run()