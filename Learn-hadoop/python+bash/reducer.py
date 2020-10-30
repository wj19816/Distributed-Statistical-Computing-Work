#! /usr/bin/python python3

import os
import sys

f = open('t.txt', 'r')
d = {}
for line in f:
    cs = line.strip()
    d[cs] = 0
f.close()


d1 = []
for line in sys.stdin:
    l = line.strip().split('\t')
    if len(l) != 4:
        continue
    [PassengerId, Survived, Sex, Age] = l
    if PassengerId in d and PassengerId not in d1:
        d1.append(PassengerId)
        res = [PassengerId, Survived]
        print('\t'.join(res))