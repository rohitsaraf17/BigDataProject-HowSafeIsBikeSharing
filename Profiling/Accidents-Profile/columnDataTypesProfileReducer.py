#!/usr/bin/env python

import sys

for line in sys.stdin:
    (key, val) = line.strip().split("\t")
    print ("%s\t%s" % (key, val))