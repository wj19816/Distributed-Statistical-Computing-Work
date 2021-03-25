#! /bin bash

# 启动准备
echo -e "\033[33m[INFO] Bigining..... \033[0m"
echo ">>>>>>>>>>>>>>>>>>> start:" `date` "<<<<<<<<<<<<<<<<<<<<<<<<<"
BIGIN_TIME=`DATE +%s`

echo -e "\n"
echo "===================================================================="
echo -e "\033[33m [INFO] 数据清洗开始 ...\033[0m "
echo "===================================================================="

# 读取与清理
spark-submit \
    --class ReadAndClean \
    --master yarn \
    "./target/DistributedDataMing-1.0-SNAPSHOT.jar" \
    "/home/data/micro_data.csv" \
    "/home/data/output/DataClean.parquet" \
    > ./data.clean.log

echo -e "\n"
echo "===================================================================="
echo -e "\033[33m [INFO] 数据清洗结束,开始建模 ...\033[0m "
echo "===================================================================="

# 建模
spark-submit \
    --class BuildModel \
    --master yarn \
    "./target/DistributedDataMing-1.0-SNAPSHOT.jar" \
    "/home/data/output/DataClean.parquet" \
    "tree" \
    > ./model.train.log

# 计算时间
END_TIME=`DATE +%s`
echo '******* Total cost' $(($END_TIME-$BIGIN_TIME)) ' second'
echo ">>>>>>>>>>>>>>>>>>> end:" `date` "<<<<<<<<<<<<<<<<<<<<<<<<<"