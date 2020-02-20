#!/usr/bin/env python

import re
import sys
infile = sys.stdin
for line in infile:
        val = line.strip()
        datas = val.split(",")
        for data in datas:
                print("%s\t%s" % (data, type(data)))
        break