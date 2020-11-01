#! /usr/env/python python3

######---------------合并missing处理结果---------------#######

import sys,re,csv
import numpy as np
import logging

# 配置日志
logging.basicConfig(filename='logger.log', level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')
logging.info('正在合并第一步预处理结果...')

missing = []
uniques = [[] for i in range(66)]

for line in sys.stdin:
    line = line.split('\tmy\t')
    missing += map(int,line[0].split(','))
    uniq = line[2].split(':;')
    uniques = [list(set(uniques[i]+uniq[i].split(','))) for i in range(66)]

var_miss = [i/missing[0] for i in missing[1:]]
uniq_num = [len(uniq) for uniq in uniques]
uniq_det = [','.join(uniq) for uniq in uniques]

print('====各变量缺失比例:====')
print(var_miss)

print('====各变量取值可能数:====')
print(uniq_num)

print('====各变量取值可能情况:====')
print(uniq_det)

