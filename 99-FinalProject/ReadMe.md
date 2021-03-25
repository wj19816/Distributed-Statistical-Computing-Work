# 【Spark系列】入门：利用Spark（Scala）对Microsoft恶意软件攻击的数据分析

[toc]

---

**本篇文章是利用`spark`对Kaggle数据集[《Microsoft Malware Prediction》](https://www.kaggle.com/c/microsoft-malware-prediction)进行了一个简单的数据分析和建模，意在实现一个完整的项目过程，并不追求模型效果。适合新手入门，从0开始快速了解流程。**

**本文由于篇幅限制，仅展示部分代码，完整项目代码见[github]()。**

**Key Word**：`spark`,`scala`,`数据分析`,`分布式计算`

---

## 一、引言

（一段废话）在计算机被全民普及的时代，计算机设备的安全性也越来越受到重视。其中，恶意软件是一种常见的能够危害计算机安全的手段，这种软件往往致力于规避传统的安全措施，而一旦计算机受到恶意软件的感染，犯罪分子会以多种方式伤害消费者和企业。Microsoft公司拥有超过10亿的企业和消费者客户，因此非常重视恶意软件攻击这一问题，并为此投入大量资金提高安全性，希望能够预测设备是否会受到恶意软件攻击，从而能够提前规避风险。

那么，如何判断你的系统是否容易受到攻击，并提前做出预警呢？**本文基于Microsoft公司提供的恶意软件数据集进行了分析。由于数据量较大，传统的计算工具难以胜任如此庞大的计算量，因此，为了更好地分析数据，本文选用了Spark框架，利用Scala语言实现的Spark对该数据集进行了数据探索与预处理，并尝试建立逻辑回归、决策树、随机森林模型对其进行分析及预测。**

## 二、数据处理

### （一）数据来源

数据集来源于Microsoft公司提供给Kaggle的数据集，数据集大小为4.08G，其中包含有变量83个，每一条样本代表了一台设备，变量HasDetections标注了该设备是否受到攻击。

### （二）数据预处理

首先在服务器上安装spark及hadoop，本文使用版本如下。

```shell
scala.version :   2.11.12
hadoop.version:   3.3.0
Spark.version :   2.4.7
```

在前几节中主要关注数据分析的过程，因此代码可以先在`spark-shell`中运行，快速获得结果。

```shell
# 开启spark-shell
spark-shell --master yarn-client \
            --executor-memory 1G \
            --num-executors 10 \
            xxx.jar
```

#### 1) 数据导入

原始数据集样本量较大，我们可以先在`shell`中使用命令`head -10000 micro_data.csv > small_data.csv` 得到一个稍小一些的数据集，并根据该小数据集对完整数据集的情况进行推断。

从小数据集中可以发现，该数据集中一共有83个变量，其中目标变量为HasDetections。**在这83个变量中，一共有73个分类变量和8个连续变量。**因此，我们根据用小数据集推断的变量信息指定了其变量类型，并匹配生成了数据集的读入格式，将完整数据集读入了spark。具体代码如下：

```scala
// 遍历获得数据类型
var schema_iter = new ArrayBuffer[StructField]( )    // 生成一个可变的Array

// 根据指定好数据类型的列表生成StructField
for (col_ <- TotalVName) {
  if (IntVName.contains(col_)) {
    schema_iter += StructField(col_, IntegerType, nullable = true)
  }
  else if (StrVName.contains(col_)) {
    schema_iter += StructField(col_, StringType, nullable = true)
  }
  else if (FloatVName.contains(col_)) {
    schema_iter += StructField(col_, FloatType, nullable = true)
  }
}
val schemaForData = StructType(schema_iter)

// 使用指定数据格式读入数据
val Micro_data: DataFrame = spark.read
		.schema(schemaForData)              //指定数据格式
		.option("header", "true")           //自动识别标题
		.csv(InputHDFS)                     //读取csv文件(仅在spark2.0+下可用)
```


计算完整数据集的变量数与样本数量，数据集中包含`8921483`个样本与`83`列。

```scala
// 计算数据集的维度
def CheckCount(df: DataFrame): Unit = {
		println("[INFO]数据集中包含  "+df.count()+"  行 与   "+df.columns.length+"  列!")
	}

CheckCount(Micro_data)
// [INFO] 数据集中包含  8921483  行 与   83  列!
```


#### 2) 数据缺失值情况

初步查看该数据集情况，数据集中存在大量缺失值。在对缺失值进行处理前首先要判断缺失机制，根据微软提供的数据说明，首先对部分变量进行了处理。例如，IsProtected变量代表了是否有至少一个活动的和最新的防病毒产品运行在这台机器上，这个变量在记录时，如果没有杀毒产品，则记为null缺失。因此，将该变量中的缺失值用0进行代替。

```scala
Micro_dum_data = Micro_dum_data.na.fill(value="0".toInt,Array("IsProtected"))
```

然后，计算各个变量的缺失值比例。

```scala
// 对于数据框中每一列计算缺失比例,并返回一个map
def CountNA(df: DataFrame): Map[String, Double] = {
  val total_n = df.count()
  var var_na_ratio: Map[String, Double] = Map()
  for(col_ <- df.columns){
    var filter_cond: String = col_ + " is null"   // 生成筛选条件
    var_na_ratio += ( col_ -> (df.select(col_)    // map中新加入一个key
                               	.filter(filter_cond).count/total_n.toDouble)    //计算某列的缺失比例
                     						.formatted("%.4f").toDouble)            // 修改数据格式
  }
  return var_na_ratio
}

var_na_ratio.foreach(println)
// [INFO] 缺失比例计算完成!
// (Census_ProcessorClass,0.9959)
// (Census_ProcessorModelIdentifier,0.0046)
// (Census_InternalPrimaryDiagonalDisplaySizeInInches,0.0053)
// (HasDetections,0.0)
// (SMode,0.0603)
// ...
```

对于某一个变量来说，缺失比例过多，在后续建模分析时其预测意义也会大大降低。因此，本文剔除了缺失比例在40%以上的变量。

```scala
println("[INFO] 以下变量缺失比例大于40%:")
// 查看缺失比例大于0.4的变量
var_na_ratio.filter((t) => t._2 > 0.4).foreach(println)  
// 将这些变量加入drop列
drop_column_mut = drop_column_mut ++ var_na_ratio.filter((t) => t._2 > 0.4).keys.toSeq  

// [INFO] 以下变量缺失比例大于40%:
// (Census_ProcessorClass,0.9959)
// (Census_ThresholdOptIn,0.6352)
// (DefaultBrowsersIdentifier,0.9514)
// (Census_InternalBatteryType,0.7105)
// (PuaMode,0.9997)
// (Census_IsWIMBootEnabled,0.6344)
// (Census_IsFlightingInternal,0.8304)
```

**表1 缺失比例较高的变量**

| 变量                       | 缺失比例 |
| -------------------------- | -------- |
| PuaMode                    | 0.9997   |
| Census_ProcessorClass      | 0.9959   |
| DefaultBrowsersIdentifier  | 0.9514   |
| Census_IsFlightingInternal | 0.8304   |
| Census_InternalBatteryType | 0.7105   |
| Census_ThresholdOptIn      | 0.6352   |
| Census_IsWIMBootEnabled    | 0.6344   |

 

#### 3) 分类变量处理

然后对分类变量进行处理。在该数据集中，一共存在有73个分类变量。但是，它们中部分是以字符的形式保存的，部分是以数值形式保存的，需要统一对它们进行处理。

首先，同样需要对部分存在问题的变量进行单独处理。如SmartScreen变量，可能由于数据采集时存在误差，出现了如`“&#x03”`之类的没有实际意义的类和存在一些大小写混乱的情况。通过实际业务理解，我们可以将部分类合并到一起。如，将`“off,of,OFF,00000000,0,&#x03;”`均记为`“Off”`。合并之后，一共留下了6个类别，更符合实际，也便于后续分析。

```scala
// 指定需要修改的字符串map
val SmartScreen_map = Map("Off" -> "off,of,OFF,00000000,0,&#x03;".split(",")
                          ,"On" -> "ON,on,Enabled".split(",")
                          ,"Block" -> "BLOCK,Deny".split(",")
                          ,"RequireAdmin" -> "requireadmin,requireAdmin,RequiredAdmin".split(",")
                          ,"Prompt" -> "Promt,Promprt,prompt".split(",")
                          ,"Warn" -> "".split(",")
                         )
// 替换原始数据
Micro_dum_data = Micro_dum_data.withColumn("SmartScreen",
			when(col("SmartScreen").isin(SmartScreen_map("Off"):_*),"Off")
				.when(col("SmartScreen").isin(SmartScreen_map("On"):_*),"On")
				.when(col("SmartScreen").isin(SmartScreen_map("Block"):_*),"Block")
				.when(col("SmartScreen").isin(SmartScreen_map("RequireAdmin"):_*),"RequireAdmin")
				.when(col("SmartScreen").isin(SmartScreen_map("Prompt"):_*),"Prompt")
				.when(col("SmartScreen").isin(SmartScreen_map("Warn"):_*),"Warn")
				.otherwise(col("SmartScreen"))
			)
```

然后，对于每一个分类变量，我们考虑以下三种情况。

1. **所有样本中有超过85%的样本取不同类别。**这种情况一般是出现在设备唯一标识id这类字符型变量上，类别太多对后续建模没有意义，因此剔除这部分分类变量。
2. **所有样本中某类占比达到85%以上。**对于这种情况，其它类的样本较少，会导致变量OneHot之后，矩阵运算会出现问题。因此剔除这个分类变量。
3. **所有类别占比均未达到5%。**如果所有类别占比均未达到5%，这意味着至少存在20种不同的类别，且不同类别占比均较小，对预测意义不大，剔除这部分变量。

在剔除这部分变量后，考虑到后续需要进行OneHot处理，因此对所有分类变量计算其各类别占比，并降序排列，保留累积占比在前85%的分类，而将剩下15%的类别记为`“zzz_others”`。具体代码如下：

```scala
// 查看分类变量取值,并进行筛选
for( col_ <- ClassVar){
	// 将各取值频数升序排列,并缓存在内存中,避免重复计算
	var cached = Micro_data.groupBy(col_).count.orderBy("count").cache()

	// 首先判断是否触发剔除条件
	if(cached.count > max_class){
		// 如果可能取值数大于85%的样本数,剔除
		drop_column_mut += col_
    
	}else if(cached.agg("count"->"max").take(1).mkString.replace("[","").replace("]","").toInt > max_class){
    // 如果单个取值可能性大于85%以上,剔除
		drop_column_mut += col_
    
	}else{
		// 如果不触发剔除条件:将分类变量的取值占比排序,最多取前85%的分类,占比小于5%及其余的分类记为"zzz_others"

		// 将分类变量的取值占比排序,最多取前85%的分类
		cached = cached.withColumn("perc", col("count") / total_n )  // 筛选占比在指定值以下的分类
		cached = cached.withColumn("cumperc", sum("perc").over(Window.orderBy(desc("perc"))))  // 计算累积占比
    
		// 获得累积在给定前85%的变量
    val keep_dumm = cached.filter(col("cumperc") <= min_cumperc)
													.select(col_).rdd.collect( )
    											.map(row => row.mkString) 
    // 获得占比大于5%的变量
		val keep_big_dumm = cached.filter(col("perc") >= min_class_perc)
															.select(col_).rdd.collect()
    													.map(row => row.mkString) 

		// 将其余分类记为"zzz_others"
		if(keep_big_dumm.length==0){
			// 如果所有类中都没有占比大于5%的,剔除该列
			drop_column_mut += col_
      
		}else{
			// 否则将过小类替换
			Micro_dum_data = Micro_dum_data.withColumn(col_,
							when(col(col_).isin(keep_dumm:_*),col(col_))
							.when(col(col_).isin(keep_big_dumm:_*),col(col_))
							.otherwise(replace_with))
		}
	}

// 取消缓存
cached.unpersist() 
```


不过，需要上述几种情况仅是非常粗糙的变量处理方式，能从大量变量中快速剔除一部分变量，并处理成能够后续建模的数据格式。如果希望得到更好的模型效果，需要结合业务理解来对变量进行处理。

#### 4) 数据导出

最后，剔除数据中包含缺失值的行，匹配模式为只要任何一列存在缺失值，即剔除该样本。

```scala
val Micro_nonull_data = Micro_pick_data.na.drop()
CheckCount(Micro_nonull_data)
// [INFO]数据集中包含  6337614  行 与   43  列!
```

经过上述预处理后，此时还剩下43个变量与6337614条样本。到目前为止，数据清洗已经基本完成，数据可以进行后续分析与建模，保存数据到parquet，以这种格式存储的数据会保留其数据格式，后续读入分析更为方便。

```scala
// 保存数据
Micro_nonull_data.write.parquet(OutputHDFS)
```



## 三、数据探索性分析

**这一步主要使用`groupBy`和`pivot`两个函数。**

### （一）因变量

首先，查看因变量HasDetections的分布。

```scala
Micro_nonull_data.groupBy(col("HasDetections")).agg(count("HasDetections")).show()

// [INFO] 因变量分布:
// +-------------+--------------------+
// |HasDetections|count(HasDetections)|
// +-------------+--------------------+
// |            1|             3125444|
// |            0|             3212170|
// s+-------------+--------------------+
```

 可以看出，在该数据集中因变量较为均衡，正负样本占比大致相等，不需要对数据进行非均衡处理。

### （二）数据透视

尝试对一些可能重要的变量进行探索性分析。

首先对AVProductsInstalled变量进行分析，这个变量代表是否安装了某个产品（具体未给予说明），从表3中可以看出当该变量取值为“1”时恶意软件攻击比例为54.38%，而当其取值为`“zzz_others”`时，恶意软件攻击比例仅为29.33%。可以推断，这个变量对于预测是否会受到恶意软件攻击有重要作用。

```scala
Micro_nonull_data.groupBy(col("AVProductsInstalled")).agg(mean("HasDetections")).show()

// +-------------------+-------------------+
// |AVProductsInstalled| avg(HasDetections)|
// +-------------------+-------------------+
// |         zzz_others|0.29337132604063654|
// |                  1| 0.5438858833733937|
// |                  2|0.40131893718663725|
// +-------------------+-------------------+
```

**表3 不同AVProductsInstalled取值时恶意软件攻击比例**

| AVProductsInstalled | avg(HasDetections) |
| ------------------- | ------------------ |
| zzz_others          | 0.2933             |
| 1                   | 0.5438             |
| 2                   | 0.4013             |

 接下来，同时查看两个变量与恶意软件攻击的关系。EngineVersion意为防卫状态信息，SmartScreen意为注册表中是否启用了SmartScreen，结果见表4。可以看出，当SmartScreen状态为ExistsNotSet且防卫状态为`1.1.15200.1`或`1.1.15100.1`时，样本中受到恶意软件攻击的比例达到了80%以上，防卫状态为其它时，攻击比例有所下降，但也有65.56%之高。而当SmartScreen取值为RequireAdmin时，恶意软件攻击比例相对较低一些。

```scala
Micro_nonull_data.groupBy("EngineVersion")
								  .pivot("SmartScreen")
									.agg(mean("HasDetections"))
									.na.fill(0).show()
```

**表4 不同EngineVersion与SmartScreen取值时恶意软件攻击比例**

| EngineVersion | ExistsNotSet | RequireAdmin | zzz_others |
| ------------- | ------------ | ------------ | ---------- |
| zzz_others    | 0.6356       | 0.3139       | 0.3904     |
| 1.1.15200.1   | 0.8273       | 0.4157       | 0.4746     |
| 1.1.15100.1   | 0.8403       | 0.4920       | 0.5103     |

## 四、建立模型

### （一）数据准备

#### 1) 数据集划分

为了后续对模型进行评估，首先对数据集进行划分，采用0.6：0.4的比例将数据集分为训练集与测试集。训练集中共有3802568条样本，测试集中有2535046条样本。为了避免后续用到该变量时出现反复计算，将数据集进行cache缓存，以减少后续重复计算。

```scala
val splits = Micro_nonull_data.randomSplit(Array(0.6, 0.4), seed = 11L)
val Train_data = splits(0).cache() // 缓存
val Test_data = splits(1).cache()
```

#### 2) OneHot处理

**对于分类变量，原始数据类型大部分为字符型，并不能直接进行建模，因此需要对分类变量进行特征数字化处理。一般常见的方式有OneHot编码，即将一个变量中的n种类别转化为n-1个变量。**

在spark中，可以利用`pipeline`工具将OneHot与后续流程进行连接。首先，将分类变量转化为数值标签，然后在进行OneHot。需要注意的是，这里参数设置为`setDropLast(true)`，这意味着在处理过程中spark会自动删除占比最小的最后一类。在前面，我们将累积占比85%以外的变量都转换成了`“zzz_others”`，但在这里却不一定会删除这一类，这也是spark的一个缺陷。

```scala
// 先转化为数值标签
val indexers:Array[PipelineStage] = pick_class_var.map { col_ =>
    new StringIndexer().setInputCol(col_)
        .setOutputCol("IDX_" + col_)
        .setHandleInvalid("keep")   //如果测试样本中存在训练样本中没有的标签，保存该标签
		}  
val onehoter = new OneHotEncoderEstimator()
    .setInputCols(pick_class_var.map("IDX_" + _ ))
    .setOutputCols(pick_class_var.map("ONEHOT_" + _ ))
    .setDropLast(true)       // drop最后一个类（并不一定是zzz_others）

// 设置特征列并转为特征向量
val featureCol = pick_conti_var++pick_class_var.map( "ONEHOT_" + _ )
val assemble = new VectorAssembler().setInputCols(featureCol).setOutputCol("features")
```

 

### （二）构建模型

接下来开始建模。本文主要使用了逻辑回归、决策树、随机森林三种模型。在对测试集进行预测时，有多种预测指标可以选择：`f1、weightedPrecision、weightedRecall、accuracy`，一般默认为f1。但是考虑到本文是对恶意软件的攻击进行预测，一旦发生恶意软件感染，对于公司的损失较大，因此公司更希望能够正确地将所有正样本预测为正，哪怕可能会将一些负样本错误判断为正样本。也就是说，在本文的建模过程中，选择召回率作为模型评价指标更为合适。

最后，由于三种模型在后续训练及预测时流程基本一致，只有在初始化模型时调用的函数不同，因此可以利用match匹配来将三种模型综合在一起，使用参数来指定模型即可。模型设定代码如下：

```scala
def SetModel(Train_data: DataFrame
             ,Test_data: DataFrame
             ,pre_pipe: Array[PipelineStage]
             ,model_type: String
             , evaluation_type: String) = {
	println("[INFO] 正在训练  "+model_type+" 模型!")
	def ModelMatch(model_type: String) = model_type match {
		case "logistic" => new LogisticRegression().setLabelCol("HasDetections")
    													.setMaxIter(10).setRegParam(0.01)
		case "tree" => new DecisionTreeClassifier().setLabelCol("HasDetections")
		case "forest" => new RandomForestClassifier().setLabelCol("HasDetections").setNumTrees(10)
	}
	var model = ModelMatch(model_type)

	// 设定模型pipeline
	val pipeline = new Pipeline().setStages(pre_pipe++Array(model))

	// 训练模型
	val Fit_Model = pipeline.fit(Train_data)
	val train_fit = Fit_Model.transform(Train_data)
	val test_fit = Fit_Model.transform(Test_data)

	// 训练结果
	println("[INFO] "+model_type+"  训练完成!")
	println("-------------- Train -------------- ")
	train_fit.select("HasDetections","probability","prediction").show(5)
	println("-------------- Test -------------- ")
	test_fit.select("HasDetections","probability","prediction").show(5)

	// 预测结果:召回率更重要 => 正确预测为正占全部正样本的比例
	println("[INFO] 正在对"+model_type+"  模型进行预测!")
	val evaluator = new MulticlassClassificationEvaluator().setLabelCol("HasDetections")
  												.setPredictionCol("prediction")
  												.setMetricName(evaluation_type)
	val train_accuracy = evaluator.evaluate(train_fit)
	println("[INFO] 测试集召回率为:   "+train_accuracy*100+" %")
	val test_accuracy = evaluator.evaluate(test_fit)
	println("[INFO] 测试集召回率为:   "+test_accuracy*100+" %")
}

// [INFO] 正在训练  tree 模型!
// [INFO] tree  训练完成!
// -------------- Train --------------
// +-------------+--------------------+----------+
// |HasDetections|         probability|prediction|
// +-------------+--------------------+----------+
// |            0|[0.63814149852460...|       0.0|
// |            1|[0.63814149852460...|       0.0|
// |            1|[0.25316691190150...|       1.0|
// |            0|[0.63814149852460...|       0.0|
// |            1|[0.25316691190150...|       1.0|
// +-------------+--------------------+----------+
// only showing top 5 rows

// -------------- Test --------------
// +-------------+--------------------+----------+
// |HasDetections|         probability|prediction|
// +-------------+--------------------+----------+
// |            1|[0.63814149852460...|       0.0|
// |            0|[0.63814149852460...|       0.0|
// |            1|[0.63814149852460...|       0.0|
// |            1|[0.25316691190150...|       1.0|
// |            1|[0.63814149852460...|       0.0|
// +-------------+--------------------+----------+
// only showing top 5 rows
                
// [INFO] 正在对tree  模型进行预测!
// [INFO] 训练集召回率为:   62.06325718701359 %
// [INFO] 测试集召回率为:   62.061297991597876 %
// [INFO] 模型预测完毕!
```


三种模型的预测召回率见表5。

**表5 三种模型的召回率**

| 数据集 | 逻辑回归 | 决策树 | 随机森林 |
| ------ | -------- | ------ | -------- |
| 训练集 | 63.73%   | 62.06% | 61.93%   |
| 测试集 | 63.69%   | 62.06% | 61.85%   |

 在这三种模型中，逻辑回归模型效果最好，训练集上的召回率为63.73%，测试集为63.69%，其次为决策树。尽管如此，所有模型的效果均不算很好，后续还可以从变量选择、模型调参等方向对整个建模过程进行改进。*（尝试过编写了交叉验证下的决策树调参模型，在小数据集上获得了测试集召回率为88.32%的模型。）*

## 五、运行与对比

### （一）框架

这一部分是本人认为对于新手来说入门最困难的一部分。因为Scala语言运行在Java虚拟机上，运行前是需要先进行编译的。前文出于调试和方便的原因主要基于spark-shell进行交互式编程。为了将代码进行重复利用，将上述代码进行整理，构建项目框架并编译成jar包。

以本文项目为例，一个完整的Spark项目文件夹应当是以下的结构：

```shell
DistributedDataMing/
├── pom.xml       # (必须)项目配置文件,用于管理：源代码结构、开发者的信息、项目的依赖关系等
├── run_main.sh   # (可选)运行主流程的代码
├── src        # (必须)源代码
│   ├── main   # (必须)主代码
│   │   ├── java
│   │   ├── resources
│   │   └── scala
│   │       ├── BuildModel.scala
│   │       └── ReadAndClean.scala
│   └── test   # (可选)测试代码
│       └── java
└── target     # (生成)编译后自动生成target文件夹,最终jar包将保存在其中
    ├── DistributedDataMing-1.0-SNAPSHOT.jar
    └── ...
```

- `pom.xml`文件中配置spark程序相关的信息与依赖包。
- `target`文件夹是编译后才生成的，编译后的`jar`包就放在该文件夹下。

- `src`中是最重要的源代码，核心代码按照包放在`main`中。
- `test`中是一些测试代码，如果想要测试某个代码是否能跑通可以放在这里，在IDEA中可以直接进行测试。
- `secource`下是一些配置文件，该项目中没有用到。

其中，对于核心代码的，如`BuildModel.scala`和`ReadAndClean.scala`，也需要格式来进行包装。示例如下：

```scala
// 导入类
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// 构建object
object BuildModel {
	def main(args: Array[String]): Unit = {
		/**
		 * 描述统计，建模
		 * @return 输出模型预测效果
		 */
    
   ...
	}
}

```

### （二）编译

`pom.xml`文件中部分配置如下：

```yml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <!-- 模型版本 -->
    <modelVersion>4.0.0</modelVersion>

    <!--基础配置-->
    <!-- 项目所有者名称,并且配置时生成的路径也是由此生成 -->
    <groupId>janeW.spark.project</groupId>
    <!-- 项目名 -->
    <artifactId>DistributedDataMing</artifactId>
    <!-- 版本号 -->
    <version>1.0-SNAPSHOT</version>
    <description>分布式系列：利用Spark进行数据分析</description>

    <!-- spark版本   -->
    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <scala.version>2.11.12</scala.version>
        <hadoop.version>3.3.0</hadoop.version>
        <spark.version>2.4.7</spark.version>
    </properties>
```

然后在根目录下利用maven命令对整个项目进行编译，生成jar包。

```shell
# 编译
mvn clean install
```

### （三）运行

然后，利用命令spark-submit提交至集群运行即可。

```shell
## 数据清洗
spark-submit \
    --class ReadAndClean \
    --master yarn \
    "./target/DistributedDataMing-1.0-SNAPSHOT.jar" \
    "/home/data/micro_data.csv" \
    "/home/data/output/DataClean.parquet" \
    > ./data.clean.log

## 建模
spark-submit \
    --class BuildModel \
    --master yarn \
    "./target/DistributedDataMing-1.0-SNAPSHOT.jar" \
    "/home/data/output/DataClean.parquet" \
    "tree" \
    > ./model.train.log
```

运行结果见日志`data.clean.log`与`model.train.log`。

## 写在最后

尽管本文中利用spark实现的数据分析过程在单机运行时也能完成，但是使用分布式系统进行数据分析可以极大地提高运行速度，但**提升比例还是依赖于代码水平以及性能调优**。由于这篇文章和项目代码是去年年底刚学spark时写的，现在看来还是存在非常多的问题，也缺少很多细节拓展。在本文所完成的报告中，有一些功能是使用`for循环`实现的，还有非常大的改进空间，并不代表最佳选择。此外，目前spark的某些功能中也存在一些问题，如前面提到的OneHot是随机剔除变量的，会与我们想要达到的效果有一点差距。在这种情况下，只能通过自己编写更为合理的程序进行改进。

如果想要快速学会使用spark分析数据，还是得尽量沉下气来从头搭建一个自己的项目，这样才能明白各个环节的原理和目的是什么。**慢即是快。**

欢迎大家提意见，让我努力产出更为实用的文章。希望能够帮到你们，谢谢！