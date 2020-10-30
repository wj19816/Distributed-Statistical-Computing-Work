#! /usr/bin/env python3

import sys

# line_i: [Survived,Sex,Age]

survive = 0
male = 0
female = 0
age = []

for line in sys.stdin:
    cols = line.strip().split('\t')
    survive += int(cols[0])
    if cols[1] == 'male':
        male += 1
    else:
        female += 1
    age.append(float(cols[2]))

avg_age = sum(age) / len(age)
print("Total Survived:{}\tMale:{}\tFemale:{}\t\t\tAll passengers's average age is {}".format(survive,male,female,round(avg_age,2)))


