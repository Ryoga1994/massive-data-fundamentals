#!/usr/bin/python34

import sys
import re

if __name__ == "__main__":

    # specify pattern as
    pat = re.compile('\[[0-9][0-9]/(.+?)/(.+?):')

    # dictionary use to match format of month
    dic = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04','May':'05','Jun':'06',
           'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

    for line in sys.stdin:
        mon,year = re.findall(pat,line)[0]
        key = "%s-%s"%(year,dic[mon])

        print("{}\t1".format(key))
