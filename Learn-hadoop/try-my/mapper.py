#! /usr/bin/env python3

import sys

i = 1

for line in sys.stdin:
    if i == 1:
        i = 0
        continue
    col = line.strip().split(',')
    cols = [col[i] for i in (1,3,4)]  #[Survived,Pclass,Sex,Age]
    line = '\t'.join(cols)
    print(line) 
