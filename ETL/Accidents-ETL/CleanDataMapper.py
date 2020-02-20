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
                if any([data[14] != "0" ,data[15] != "0",data[24].lower() == transportType,data[25].lower() == transportType,data[26].lower() == transportType,data[27].lower() == transportType,data[28].lower() == transportType]):
                        data[0] = data[0].replace("-","/")
                        values = [data[0],data[1],data[4],data[5],data[14],data[15],data[24].lower(),data[25].lower(),data[26].lower(),data[27].lower(),data[28].lower()]
                        keyDatas = data[0].split("/")
                        print("%s\t%s" % (keyDatas[2]+"/"+keyDatas[0],values))