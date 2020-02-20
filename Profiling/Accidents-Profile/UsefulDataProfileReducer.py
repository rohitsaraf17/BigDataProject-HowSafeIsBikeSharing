#!/usr/bin/env python

import sys

total = 0
actual = 0
for line in sys.stdin:
        (key, val) = line.strip().split("\t")
        if val == "1":
                actual = actual + 1
        total = total + 1
print("Total Data = %d\nUseful Data = %d" % (total,actual))