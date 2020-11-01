#! /usr/env/python python3

######---------------缺失值处理、变量剔除及数据格式转换---------------#######

import sys,re,csv
import numpy as np
import logging

# 配置日志
logging.basicConfig(filename='logger.log', level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')
logging.info('Get ready to clean the data...')

# 设置变量初始值
isnull = [None,'',' ','-','--']
unit = re.compile(r'(?<=\d) (in|seats|gal|RPM)')

drop_list = [0, 2, 3, 4, 6, 7, 9, 11, 12, 13, 15, 16, 20, 28, 30, 31, 33, 36, 37, 38, 40, 41, 42, 45, 46, 52, 53, 57, 58, 59, 60]
num_list = [1, 8, 10, 14, 21, 22, 25, 26, 27, 34, 35, 39, 43, 44, 48, 50, 51, 63, 64, 65]
cla_list = [5, 17, 18, 19, 23, 24, 29, 32, 49, 54, 56, 61, 62]

# 理论上经过处理后的list
drop_list += [47, 55] + [5,23,56,61,62]
num_list  += [66, 67, 68, 69]

# 分类变量
body_type = ['Minivan','Coupe','Pickup Truck','SUV / Crossover','Hatchback','Wagon','Sedan','Convertible','Van','Others']
fuel_type = ['Biodiesel','Gasoline','Electric','Diesel','Flex Fuel Vehicle','Hybrid','Others']
transmission = ['A','Dual Clutch','CVT','M','Others']
wheel_system = ['4WD','FWD','RWD','4X2','AWD','Others']
listing_color = ['YELLOW','SILVER','RED','WHITE','BLUE','UNKNOWN','BROWN','ORANGE','PURPLE','BLACK','GREEN','TEAL','GRAY','GOLD','Others']
wheel_system_display = ['Four-Wheel Drive','Front-Wheel Drive','4X2','Rear-Wheel Drive','All-Wheel Drive','Others']

def get_dummy(li,lst):
    #针对分类变量生成哑变量
    if li in [None,'',' ','-','--']:
        return [0]*len(lst)
    else:
        dummy_new = [0]*len(lst)
        index = lst.index(li) if li in lst else -1  #如果不在备选列表中,记为Others
        dummy_new[index] = 1
        return dummy_new

# 开始读取数据
lines = csv.reader(sys.stdin)
for line in lines:
    if line[0] == 'vin': 
        continue
    
    try: 
        line = [i.strip().strip('\n') for i in line]
        assert len(line) == 66  #判断正则表达式是否匹配正确

        # 去除单位
        line = [re.sub('--', '', i) for i in line]
        line = [re.sub(unit, '', i) for i in line]

        # 逻辑分类变量处理
        line = [re.sub('FALSE', '0', i, flags=re.IGNORECASE) for i in line]
        line = [re.sub('TRUE' , '1', i, flags=re.IGNORECASE) for i in line]

        # 47,55 新增一个变量
        power  = re.split(r' hp @ ', re.sub('[",]', '', line[47]))
        torque = re.split(r' lb-ft @ ', re.sub('[",]', '', line[55]))
        line  += power + torque

        # 分类变量处理
        line += get_dummy(line[5],body_type)  # body_type:5 [70-79] 10
        line += get_dummy(line[23],fuel_type)  # fuel_type:23 [80-86] 7
        line += get_dummy(line[56],transmission)  # transmission:56 [87-91] 6
        line += get_dummy(line[61],wheel_system)  # wheel_system:61 [92-97] 6
        line += get_dummy(line[62],wheel_system_display)  # wheel_system_display:62 [98-103] 6

        # 处理数值变量
        # line = [float(line[i]) if i in num_list and line[i] not in isnull else line[i] for i in range(len(line))]
        
        # 剔除变量
        line = [str(line[i]) for i in range(len(line)) if i not in drop_list]

        if len(line) == 66 :
            print(','.join(line))

    except:
        continue