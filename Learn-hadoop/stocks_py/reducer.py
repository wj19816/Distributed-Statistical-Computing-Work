#! /usr/bin/env python3

# get daily returns

import sys

for line in sys.stdin:
    #if line == '\n':
    #    break
    s = line.split(',')
    opens = float(s[2])
    close = float(s[5])
    earn = (close-opens)/opens*100
    print('Stock name:',s[0],'    Date:',s[1],'    Today\'s yield:',round(earn,2),'%')

