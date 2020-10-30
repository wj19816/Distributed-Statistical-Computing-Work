#!/bin/bash
echo '>>>>>>>>>>>>>>>> start:' `date`
BEGIN_TIME=`date +%s`

set -u

num=$#
if [ $num -ne 4 ]; then
    echo -e "This program is designed for 1 mapper + 1 reducer + 1 inputfile."
    echo -e "please input the mapper file,reducer file ,input file and output path: sh main.sh mapper reducer inputfile outputpath\n"
    exit 1
else
    MAPPER=${1}
    REDUCER=${2}
    INPUTFILE=${3}
    OUTPUTPATH=${4}
fi

# main dir
PWD=$(cd $(dirname $0); pwd)
cd $PWD 1> /dev/null 2>&1

TASKNAME=task_wj

# hadoop client
HADOOP_HOME=/usr/lib/hadoop-current
HADOOP_PREFIX=/user/devel/2020211027wangjing
HADOOP_INPUT_DIR=${HADOOP_PREFIX}/${INPUTFILE}
HADOOP_OUTPUT_DIR=${HADOOP_PREFIX}/output/${OUTPUTPATH}

echo $HADOOP_HOME
echo $HADOOP_INPUT_DIR
echo $HADOOP_OUTPUT_DIR

hadoop fs -rmr $HADOOP_OUTPUT_DIR #删除已有的output文件夹

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.1.3.jar \
    -input ${HADOOP_INPUT_DIR} \
    -output ${HADOOP_OUTPUT_DIR} \
    -file ${MAPPER} ${REDUCER} \
    -mapper ${MAPPER} \
    -reducer ${REDUCER}

if [ $? -ne 0 ]; then
    echo 'error'
    exit 1
fi
#hadoop fs -touchz ${HADOOP_OUTPUT_DIR}/done  #如果成功,生成一个文件夹

END_TIME=`date +%s`

echo '******Total cost '  $(($END_TIME-$BEGIN_TIME)) ' seconds'
echo '>>>>>>>>>>>>>>>> end:' `date`
echo "=============SUCCESSFUL============="

exit 0
