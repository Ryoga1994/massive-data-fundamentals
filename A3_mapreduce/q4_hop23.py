#!/usr/bin/python34
#
# Template to compute the number of distinct IP addresses
#

from mrjob.job import MRJob
import re
import statistics


line = '2016-07-01T00:01:01,1,,192.168.10.1,1798,2,,96.120.104.177,9739,3,,68.87.130.233,11766,4,ae-53-0-ar01.capitolhghts.md.bad.comcast.net,68.86.204.217,11203,5,be-33657-cr02.ashburn.va.ibone.comcast.net,68.86.90.57,14575,6,he-0-2-0-0-ar01-d.westchester.pa.bo.comcast.net,68.86.94.226,17923,7,bu-101-ur21-d.westchester.pa.bo.comcast.net,68.85.137.213,16070,8,,68.87.29.59,16761'

class LinkDev23(MRJob):

    def mapper(self, _, line):
        # Your code goes here

        pat_time = re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}T([0-9]{2}):[0-9]{2}:[0-9]{2}')
        pat = re.compile('([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+),([0-9]+),*')

        # extract timestamp, ipaddress, microseconds for response from Netflix
        pairs = pat.findall(line)
        timestamp_hour = pat_time.findall(line)

        # with more than 2 hops
        if(len(pairs)>2):
            yield timestamp_hour[0],int(pairs[2][1])-int(pairs[1][1]) # link, time


    def reducer(self, time, values):
        # Your code goes here

        lis_time = []

        for v in values:
            lis_time.append(int(v))

        yield int(time),statistics.pstdev(lis_time)


if __name__ == '__main__':
    LinkDev23.run()