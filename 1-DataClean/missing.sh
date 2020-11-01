#!/bin/bash
echo "===================================================================="
echo -e "\033[33m [INFO] 计算各变量缺失值个数及可能取值数 \033[0m "
echo "===================================================================="

echo '>>>>>>>>>>>>>>>> start:' `date`
BEGIN_TIME=`date +%s`

# main dir
PWD=$(cd $(dirname $0); pwd)
cd $PWD 1> /dev/null 2>&1

TASKNAME=task_wj

# hadoop client
HADOOP_HOME=/usr/lib/hadoop-current
HADOOP_PREFIX=/user/devel/2020211027wangjing
HADOOP_INPUT_DIR=${HADOOP_PREFIX}/sample_10000.csv
HADOOP_OUTPUT_DIR=${HADOOP_PREFIX}/output/1015-preprocess/missing

echo $HADOOP_HOME
echo $HADOOP_INPUT_DIR
echo $HADOOP_OUTPUT_DIR

hadoop fs -rmr $HADOOP_OUTPUT_DIR #删除已有的output文件夹

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.1.3.jar \
    -input ${HADOOP_INPUT_DIR} \
    -output ${HADOOP_OUTPUT_DIR} \
    -file 'missing.py' 'merge.py' \
    -mapper "python3 missing.py" \
    -reducer "python3 merge.py"

if [ $? -ne 0 ]; then
    echo 'error'
    exit 1
fi

END_TIME=`date +%s`

echo '******Total cost '  $(($END_TIME-$BEGIN_TIME)) ' seconds'
echo '>>>>>>>>>>>>>>>> end:' `date`
echo "=============SUCCESS=============="

echo "===================================================================="
echo -e "\033[33m [INFO] 各变量缺失值个数及可能取值数为: \033[0m "
hadoop fs -cat ${HADOOP_OUTPUT_DIR}/*
echo "===================================================================="

exit 0