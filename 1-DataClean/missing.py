#! /usr/env/python python3

######---------------计算各变量缺失值个数,可能取值数及取值情况---------------#######

import sys,re,csv
import numpy as np
import logging

# 配置日志
logging.basicConfig(filename='logger.log', level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')
logging.info('======Start!=====')
logging.info('Get ready to count missing values...')

# 设置变量初始值
count = 0
uniques = [[] for i in range(66)]
missing = np.array([0 for i in range(66)])
isnull = [None,'',' ','-','--']

# 开始读取数据
lines = csv.reader(sys.stdin)
for line in lines:
    n = len(line)
    if line[0] == 'vin': 
        continue
    
    try: 
        assert n == 66  #判断正则表达式是否匹配正确
        count += 1

        uniques = [uniques[i]+[line[i]] if line[i] not in uniques[i] else uniques[i] for i in range(n)]
        miss_ind = [i in isnull for i in line ]
        missing[miss_ind] += 1

    except:
        continue

# 生成输出数据
var_miss = [str(x) for x in np.insert(missing,0,count)]
var_out = ','.join(var_miss)

uniq_num = [str(len(uniq)) for uniq in uniques]
uniq_num_out = ','.join(uniq_num)

uniq_det = [','.join(uniq) for uniq in uniques]
uniq_out = ':;'.join(uniq_det)

# 输出格式: 缺失值数 \t 可能取值数 \t 所有可能取值列表(var1;var2;...;varN)
# count,missing1,missing2,...,missingN \t  uniq1,uniq2,...,uniqN \t u1,u2,u3;u1;u2,u3;...
print(var_out,'\tmy\t',uniq_num_out,'\tmy\t',uniq_out)