import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel,PipelineStage}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.{DecisionTreeClassifier,DecisionTreeClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator  // 用于评估结果

/**
 * Created by Jane on 2020/12/20
 */


object BuildModel {
	def main(args: Array[String]): Unit = {
		/**
		 * 描述统计，建模
		 * @return 输出模型预测效果
		 */

		// 开启spark
		val spark = SparkSession.builder.master("local").appName("spark-build-model").getOrCreate
		spark.conf.set("spark.sql.debug.maxToStringFields", 100) //设置最大字符串长度

		// 读取数据
		val OutputHDFS = args(0)
		val model_type = args(1)
		val Micro_nonull_data = spark.read.load(OutputHDFS)

		/**
		 * 描述统计
		 */

		// 指定变量类型
		val pick_class_var = "EngineVersion,AppVersion,AVProductStatesIdentifier,AVProductsInstalled,OrganizationIdentifier,GeoNameIdentifier,LocaleEnglishNameIdentifier,OsBuild,OsSuite,OsPlatformSubRelease,OsBuildLab,SkuEdition,IeVerIdentifier,SmartScreen,Census_MDC2FormFactor,Census_OEMNameIdentifier,Census_PrimaryDiskTypeName,Census_ChassisTypeName,Census_PowerPlatformRoleName,Census_OSVersion,Census_OSBranch,Census_OSBuildNumber,Census_OSBuildRevision,Census_OSEdition,Census_OSSkuName,Census_OSInstallTypeName,Census_OSInstallLanguageIdentifier,Census_OSUILocaleIdentifier,Census_OSWUAutoUpdateOptionsName,Census_ActivationChannel,Census_FirmwareManufacturerIdentifier,Census_IsSecureBootEnabled,Wdft_IsGamer,Wdft_RegionIdentifier".split(",")
		val pick_conti_var = "Census_ProcessorCoreCount,Census_PrimaryDiskTotalCapacity,Census_SystemVolumeTotalCapacity,Census_TotalPhysicalRAM,Census_InternalPrimaryDiagonalDisplaySizeInInches,Census_InternalPrimaryDisplayResolutionHorizontal,Census_InternalPrimaryDisplayResolutionVertical,Census_InternalBatteryNumberOfCharges".split(",")
		val target_var = "HasDetections"
		val pick_var = pick_class_var ++ pick_conti_var

		// 因变量分布
		println("[INFO] 因变量分布:")
		Micro_nonull_data.groupBy(col("HasDetections")).count.show()  // 较为均衡,几乎为1:1

		// 数据透视
		// 若干个变量在不同取值下的被攻击的比例
		println("[INFO] 正在计算不同类别下计算机被攻击的比例！")
		Micro_nonull_data.groupBy(col("Census_IsSecureBootEnabled")).agg(mean("HasDetections")).show()
		Micro_nonull_data.groupBy(col("Census_ActivationChannel")).agg(mean("HasDetections")).show()
		Micro_nonull_data.groupBy(col("AVProductsInstalled")).agg(mean("HasDetections")).show()
		Micro_nonull_data.groupBy(col("Census_OSWUAutoUpdateOptionsName")).agg(mean("HasDetections")).show()

		println("[INFO] 正在尝试数据透视！")
		Micro_nonull_data.groupBy("Census_ActivationChannel").pivot("Census_IsSecureBootEnabled").agg(mean("HasDetections")).na.fill(0).show()
		Micro_nonull_data.groupBy("EngineVersion").pivot("SmartScreen").agg(mean("HasDetections")).na.fill(0).show()
		Micro_nonull_data.groupBy("AVProductsInstalled").pivot("Census_IsSecureBootEnabled").agg(mean("HasDetections")).na.fill(0).show()

		/**
		 * 建模
		 */

		// 分类变量处理:ONEHOT
		println("[INFO] 正在对分类变量进行onehot处理!")

		// 先转化为数值标签
		val indexers:Array[PipelineStage] = pick_class_var.map { col_ =>
			new StringIndexer().setInputCol(col_)
				.setOutputCol("IDX_" + col_)
				.setHandleInvalid("keep")}  //如果测试样本中存在训练样本中没有的标签，保存该标签
		val onehoter = new OneHotEncoderEstimator()
			.setInputCols(pick_class_var.map("IDX_" + _ ))
			.setOutputCols(pick_class_var.map("ONEHOT_" + _ ))
			.setDropLast(true)   // drop最后一个类（并不一定是zzz_others）

		// 设置特征列并转为特征向量
		val featureCol = pick_conti_var++pick_class_var.map( "ONEHOT_" + _ )
		val assemble = new VectorAssembler().setInputCols(featureCol).setOutputCol("features")

		println("[INFO] 正在拆分数据集!")
		// 拆分数据集
		val splits = Micro_nonull_data.randomSplit(Array(0.6, 0.4), seed = 11L)
		val Train_data = splits(0).cache() // 缓存
		val Test_data = splits(1).cache()

		val pre_pipe = indexers++Array(onehoter,assemble)

		println("[INFO] 开始建模!")
		val evaluation_type = "weightedRecall"  // "f1", "weightedPrecision", "weightedRecall", "accuracy"
		SetModel(Train_data,Test_data,pre_pipe,model_type,evaluation_type)

		// 释放内存
		Train_data.unpersist()
		Test_data.unpersist()
		println("[INFO] 模型预测完毕!")

	}

	// 设定模型
	def SetModel(Train_data: DataFrame,Test_data: DataFrame,pre_pipe: Array[PipelineStage],
	             model_type: String, evaluation_type: String) = {
		println("[INFO] 正在训练  "+model_type+" 模型!")

		// 匹配模型类型
		def ModelMatch(model_type: String) = model_type match {
			case "logistic" => new LogisticRegression().setLabelCol("HasDetections").setMaxIter(10).setRegParam(0.01)
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
		val evaluator = new MulticlassClassificationEvaluator().setLabelCol("HasDetections").setPredictionCol("prediction").setMetricName(evaluation_type)
		val train_accuracy = evaluator.evaluate(train_fit)
		println("[INFO] 训练集召回率为:   "+train_accuracy*100+" %")
		val test_accuracy = evaluator.evaluate(test_fit)
		println("[INFO] 测试集召回率为:   "+test_accuracy*100+" %")
	}
}
