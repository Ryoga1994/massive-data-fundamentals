#!/usr/bin/python34

import sys
import re

if __name__ == "__main__":

    # find url
    pat = re.compile('\"(http://.+?)"')

    for line in sys.stdin:
        url = re.findall(pat,line)[0]

        print("{}\t1".format(url))


