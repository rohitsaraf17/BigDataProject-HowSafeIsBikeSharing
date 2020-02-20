#!/usr/bin/env python

import re
import sys
infile = sys.stdin
next(infile)
transportType = "bicycle"
for line in infile:
        val = line.strip()
        data = val.split(",")
        if (len(data) == 29):
                if any([data[14] !="0" ,data[15] != "0",data[24].lower() == transportType,data[25].lower() == transportType,data[26].lower() == transportType,data[27].lower() == transportType,data[28].lower() == transportType]):
                        print("%s\t%s" % ("Info", 1))
        print("%s\t%s" % ("Info", 0))