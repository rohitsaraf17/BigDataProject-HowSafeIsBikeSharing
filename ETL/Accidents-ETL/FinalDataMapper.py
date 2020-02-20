#!/usr/bin/env python

import re
import sys
infile = sys.stdin
for line in infile:
        val = line.strip()
        data = val.split(",")
        first_data = data[0].split(" ")
        date = first_data.split("/")
        year = date[0]
        month = date[1]
        zip = data[4]
        vehicle1 = data[7]
        vehicles = vehicle1.split("/")
        for vehicle in vehicles
            print("%s\t%s" % (year,[month,zip,vehicles]))