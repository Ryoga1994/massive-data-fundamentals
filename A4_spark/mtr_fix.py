#!/usr/bin/env python35
#
# Parse an MTR file in text mode and turn it into a tinydata file.
#
import sys
if sys.version < "3.4":
    raise RuntimeError("requires Pyton 3.4 or above")

import os,re
import csv
from datetime import datetime

#line = '1.|-- 192.168.10.1               0.0%     1    1.6   1.6   1.6   1.6   0.0'
# line = '7.|-- ???                       100.0     1    0.0   0.0   0.0   0.0   0.0'
mtr_line_exp = re.compile("(\d+?).\|--\s+(.+?)\s+([^\s\%]+)[\%]*\s+\d+\s+([\d.]+)\s")   # put something here
# mtr_line_exp.findall(line)
# line = 'Start: Sun Jan  1 00:00:28 2017'

def parse_timestamp(line):
    assert line.startswith("Start:")
    # ignore label of weekday
    val = line.lstrip('Start:').strip()

    date_time = datetime.strptime(val, "%a %b %d %H:%M:%S %Y")

    return date_time.isoformat()

# parse_timestamp(line)

# line = ' 4.|-- ae-53-0-ar01.capitolhghts  100.0%     1   10.8  10.8  10.8  10.8   0.0'
line = 'Start: Sun Jan 15 19:01:28 2017'

class MtrLine:
    def __init__(self,ts,line):
        assert re.match(mtr_line_exp,line)

        # Parse line and fill these in.
        # ts is the timestamp that we previously found
        line = line.strip()
        self.timestamp = ts
        self.hop_number = ''
        self.ipaddr = ''
        self.hostname = ''
        self.pct_loss = ''
        self.time = ''

        pat_ip = re.compile('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
        pat_hostname = re.compile('[a-zA-Z]{2}-.+')

        res = mtr_line_exp.findall(line)[0]

        self.hop_number = int(res[0])

        # parse ip or hostname
        if pat_ip.match(res[1]):
            self.ipaddr = res[1]
            for key in ip_host:
                if key == self.ipaddr:
                    self.hostname = ip_host[key]

        elif pat_hostname.match(res[1]):
            self.hostname = res[1]
            # find complete hostname
            for key in host_ip:
                if key.startswith(res[1]):
                    self.hostname = key
                    self.ipaddr = host_ip[key]

        self.pct_loss = int(float(res[2]))

        self.time = round(float(res[3]),2)



def host_ip_dic():
    # input s3://gu-anly502/A3/mtr.www.comcast.com.2016.txt and
    # create dictionary for all the <hostname, ip>
    host_ip = {}
    ip_host = {}

    pat = re.compile('[0-9]+,([^,]+?),([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+),[0-9]+,')

    with open('mtr.records.2016.txt','r') as f:
        reader = f.readlines()

        for row in reader:
            lists = pat.findall(row.strip())
            for lis in lists:
                host_ip[lis[0]] = lis[1]
                ip_host[lis[1]] = lis[0]

    return (host_ip,ip_host)

def fix_mtr(infile,outfile):
    count = 0
    current_timestamp = None
    for line in infile:
        line = line.strip()     # remove leading and trailing white space
        line = line.replace('\x00', '')
        if line.startswith('Start:'):
            # Beginning of a new record...
            # print("Replace this print statement with new code. Probably need to set a variable with the time...")
            ts = parse_timestamp(line)
            continue

        if line.startswith('HOST:'):
            # This can be ignored, since we always start at the same location
            continue

        # print(line)
        m= MtrLine(ts,line)
        if m.timestamp:
            # print("Regular expression matched. Replace this with code...")
            outfile.write("%s,%d,%s,%s,%d,%s\n"%(m.timestamp,m.hop_number,m.ipaddr,m.hostname,m.pct_loss,m.time))
            count += 1
    return count

        

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--infile",help="input file",default="mtr.www.cnn.com.txt")
    parser.add_argument("--outfile",help="output file",default="tidy_data.txt")
    args = parser.parse_args()

    if os.path.exists(args.outfile):
        raise RuntimeError(args.outfile + " already exists. Please delete it.")

    # input dictionary as (hostname, ip address)
    host_ip,ip_host = host_ip_dic()

    print("{} -> {}".format(args.infile,args.outfile))

    count = fix_mtr(open(args.infile,"rU"), open(args.outfile,"w"))
    print("{} records converted".format(count))
