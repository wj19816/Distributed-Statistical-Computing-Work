import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Created by Jane on 2020/12/20
 */

object ReadAndClean {
	def main(args: Array[String]): Unit = {
		/**
		 * 读取数据,并进行数据清洗
		 * @return 将清洗后的数据存入HDFS
		 */

		// 开启spark
		val spark = SparkSession.builder.master("local").appName("spark-data-clean").getOrCreate
		spark.conf.set("spark.sql.debug.maxToStringFields", 100) //设置最大字符串长度

		/**
		 * 数据导入
		 */

		// 传入参数
		val InputHDFS = args(0)
		val OutputHDFS = args(1)

		// 变量数据类型
		val TotalVName = "MachineIdentifier,ProductName,EngineVersion,AppVersion,AvSigVersion,IsBeta,RtpStateBitfield,IsSxsPassiveMode,DefaultBrowsersIdentifier,AVProductStatesIdentifier,AVProductsInstalled,AVProductsEnabled,HasTpm,CountryIdentifier,CityIdentifier,OrganizationIdentifier,GeoNameIdentifier,LocaleEnglishNameIdentifier,Platform,Processor,OsVer,OsBuild,OsSuite,OsPlatformSubRelease,OsBuildLab,SkuEdition,IsProtected,AutoSampleOptIn,PuaMode,SMode,IeVerIdentifier,SmartScreen,Firewall,UacLuaenable,Census_MDC2FormFactor,Census_DeviceFamily,Census_OEMNameIdentifier,Census_OEMModelIdentifier,Census_ProcessorCoreCount,Census_ProcessorManufacturerIdentifier,Census_ProcessorModelIdentifier,Census_ProcessorClass,Census_PrimaryDiskTotalCapacity,Census_PrimaryDiskTypeName,Census_SystemVolumeTotalCapacity,Census_HasOpticalDiskDrive,Census_TotalPhysicalRAM,Census_ChassisTypeName,Census_InternalPrimaryDiagonalDisplaySizeInInches,Census_InternalPrimaryDisplayResolutionHorizontal,Census_InternalPrimaryDisplayResolutionVertical,Census_PowerPlatformRoleName,Census_InternalBatteryType,Census_InternalBatteryNumberOfCharges,Census_OSVersion,Census_OSArchitecture,Census_OSBranch,Census_OSBuildNumber,Census_OSBuildRevision,Census_OSEdition,Census_OSSkuName,Census_OSInstallTypeName,Census_OSInstallLanguageIdentifier,Census_OSUILocaleIdentifier,Census_OSWUAutoUpdateOptionsName,Census_IsPortableOperatingSystem,Census_GenuineStateName,Census_ActivationChannel,Census_IsFlightingInternal,Census_IsFlightsDisabled,Census_FlightRing,Census_ThresholdOptIn,Census_FirmwareManufacturerIdentifier,Census_FirmwareVersionIdentifier,Census_IsSecureBootEnabled,Census_IsWIMBootEnabled,Census_IsVirtualDevice,Census_IsTouchEnabled,Census_IsPenCapable,Census_IsAlwaysOnAlwaysConnectedCapable,Wdft_IsGamer,Wdft_RegionIdentifier,HasDetections".split(',')
		val IntVName = "HasDetections,IsBeta,RtpStateBitfield,IsSxsPassiveMode,DefaultBrowsersIdentifier,AVProductStatesIdentifier,AVProductsInstalled,AVProductsEnabled,HasTpm,CountryIdentifier,CityIdentifier,OrganizationIdentifier,GeoNameIdentifier,LocaleEnglishNameIdentifier,OsBuild,OsSuite,IsProtected,AutoSampleOptIn,SMode,IeVerIdentifier,Firewall,UacLuaenable,Census_OEMNameIdentifier,Census_OEMModelIdentifier,Census_ProcessorCoreCount,Census_ProcessorManufacturerIdentifier,Census_ProcessorModelIdentifier,Census_PrimaryDiskTotalCapacity,Census_SystemVolumeTotalCapacity,Census_HasOpticalDiskDrive,Census_TotalPhysicalRAM,Census_InternalPrimaryDisplayResolutionHorizontal,Census_InternalPrimaryDisplayResolutionVertical,Census_InternalBatteryNumberOfCharges,Census_OSBuildNumber,Census_OSBuildRevision,Census_OSInstallLanguageIdentifier,Census_OSUILocaleIdentifier,Census_IsPortableOperatingSystem,Census_IsFlightingInternal,Census_IsFlightsDisabled,Census_ThresholdOptIn,Census_FirmwareManufacturerIdentifier,Census_FirmwareVersionIdentifier,Census_IsSecureBootEnabled,Census_IsWIMBootEnabled,Census_IsVirtualDevice,Census_IsTouchEnabled,Census_IsPenCapable,Census_IsAlwaysOnAlwaysConnectedCapable,Wdft_IsGamer,Wdft_RegionIdentifier".split(',')
		val StrVName = "MachineIdentifier,ProductName,EngineVersion,AppVersion,AvSigVersion,Platform,Processor,OsVer,OsPlatformSubRelease,OsBuildLab,SkuEdition,PuaMode,SmartScreen,Census_MDC2FormFactor,Census_DeviceFamily,Census_ProcessorClass,Census_PrimaryDiskTypeName,Census_ChassisTypeName,Census_PowerPlatformRoleName,Census_InternalBatteryType,Census_OSVersion,Census_OSArchitecture,Census_OSBranch,Census_OSEdition,Census_OSSkuName,Census_OSInstallTypeName,Census_OSWUAutoUpdateOptionsName,Census_GenuineStateName,Census_ActivationChannel,Census_FlightRing".split(',')
		val FloatVName = "Census_InternalPrimaryDiagonalDisplaySizeInInches".split(',')

		// 遍历获得数据类型
		var schema_iter = new ArrayBuffer[StructField]()
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
		var Micro_data: DataFrame = spark.read
			.schema(schemaForData)            //指定数据格式
			.option("header", "true")         //自动识别标题
			.csv(InputHDFS)  //读取csv文件(仅在spark2.0+下可用)

		// 查看数据格式
		Micro_data.printSchema()  //这样读入的数据是按照数据格式的顺序排的

		// 查看样本个数及变量数
		val total_n = Micro_data.count()
		val total_v = Micro_data.columns.length
		CheckCount(Micro_data)

		println("[INFO]======原始数据读取完成!======")
		Micro_data.show(3)

		/**
		 * 数据清洗
		 */

		// 指定分类变量及连续变量:73个分类变量,8个数值变量,1个因变量
		val ClassVar = "MachineIdentifier,ProductName,EngineVersion,AppVersion,AvSigVersion,IsBeta,RtpStateBitfield,IsSxsPassiveMode,DefaultBrowsersIdentifier,AVProductStatesIdentifier,AVProductsInstalled,AVProductsEnabled,HasTpm,CountryIdentifier,CityIdentifier,OrganizationIdentifier,GeoNameIdentifier,LocaleEnglishNameIdentifier,Platform,Processor,OsVer,OsBuild,OsSuite,OsPlatformSubRelease,OsBuildLab,SkuEdition,IsProtected,AutoSampleOptIn,PuaMode,SMode,IeVerIdentifier,SmartScreen,Firewall,UacLuaenable,Census_MDC2FormFactor,Census_DeviceFamily,Census_OEMNameIdentifier,Census_OEMModelIdentifier,Census_ProcessorManufacturerIdentifier,Census_ProcessorModelIdentifier,Census_ProcessorClass,Census_PrimaryDiskTypeName,Census_HasOpticalDiskDrive,Census_ChassisTypeName,Census_PowerPlatformRoleName,Census_InternalBatteryType,Census_OSVersion,Census_OSArchitecture,Census_OSBranch,Census_OSBuildNumber,Census_OSBuildRevision,Census_OSEdition,Census_OSSkuName,Census_OSInstallTypeName,Census_OSInstallLanguageIdentifier,Census_OSUILocaleIdentifier,Census_OSWUAutoUpdateOptionsName,Census_IsPortableOperatingSystem,Census_GenuineStateName,Census_ActivationChannel,Census_IsFlightingInternal,Census_IsFlightsDisabled,Census_FlightRing,Census_ThresholdOptIn,Census_FirmwareManufacturerIdentifier,Census_FirmwareVersionIdentifier,Census_IsSecureBootEnabled,Census_IsWIMBootEnabled,Census_IsVirtualDevice,Census_IsTouchEnabled,Census_IsPenCapable,Census_IsAlwaysOnAlwaysConnectedCapable,Wdft_IsGamer,Wdft_RegionIdentifier".split(",")
		val NumVar = "Census_SystemVolumeTotalCapacity,Census_InternalBatteryNumberOfCharges,Census_InternalPrimaryDiagonalDisplaySizeInInches,Census_PrimaryDiskTotalCapacity,Census_TotalPhysicalRAM,Census_InternalPrimaryDisplayResolutionHorizontal,Census_InternalPrimaryDisplayResolutionVertical,Census_ProcessorCoreCount".split(",")
		val TargetVar = "HasDetections"
		val FeatureVar = ClassVar ++ NumVar

		// 设定一个array储存需要剔除的列(这是一个mutable array)
		var drop_column_mut = ArrayBuffer[String]()

		// 对一些特殊特殊变量进行处理
		var Micro_dum_data = Micro_data

		// IsProtected: null改为0
		Micro_dum_data = Micro_dum_data.na.fill(value="0".toInt,Array("IsProtected"))

		// SmartScreen: 错误记录替换
		// 指定替换map
		val SmartScreen_map = Map("Off" -> "off,of,OFF,00000000,0,&#x03;".split(",")
								,"On" -> "ON,on,Enabled".split(",")
								,"Block" -> "BLOCK,Deny".split(",")
								,"RequireAdmin" -> "requireadmin,requireAdmin,RequiredAdmin".split(",")
								,"Prompt" -> "Promt,Promprt,prompt".split(",")
								,"Warn" -> "".split(",")
		)
		Micro_dum_data = Micro_dum_data.withColumn("SmartScreen",
			when(col("SmartScreen").isin(SmartScreen_map("Off"):_*),"Off")
				.when(col("SmartScreen").isin(SmartScreen_map("On"):_*),"On")
				.when(col("SmartScreen").isin(SmartScreen_map("Block"):_*),"Block")
				.when(col("SmartScreen").isin(SmartScreen_map("RequireAdmin"):_*),"RequireAdmin")
				.when(col("SmartScreen").isin(SmartScreen_map("Prompt"):_*),"Prompt")
				.when(col("SmartScreen").isin(SmartScreen_map("Warn"):_*),"Warn")
				.otherwise(col("SmartScreen"))
		)

		// 查询各列的缺失比例
		val var_na_ratio = CountNA(Micro_data)
		println("[INFO] 缺失比例计算完成!")

		// 输出各变量的缺失比例
		var_na_ratio.foreach(println)

		// 剔除缺失比例大于0.4的变量
		println("[INFO] 以下变量缺失比例大于40%:")
		var_na_ratio.filter((t) => t._2 > 0.4).foreach(println)  // 查看缺失比例大于0.4的变量
		drop_column_mut = drop_column_mut ++ var_na_ratio.filter((t) => t._2 > 0.4).keys.toSeq  // 将这些变量加入drop列

		// 设定阈值
		val min_cumperc = 0.85   // 最小累积比
		val min_class_perc = 0.05   // 最小分类占比
		val max_class = total_n * min_cumperc  // 最大某分类占比
		val min_class = total_n * min_class_perc  // 最小某分类占比
		val replace_with = "zzz_others"

		// 查看分类变量取值,并进行筛选
		for( col_ <- ClassVar){
			// 将各取值频数升序排列,并缓存在内存中,避免重复计算
			var cached = Micro_data.groupBy(col_).count.orderBy("count").cache()

			// 首先判断是否触发剔除条件
			if(cached.count > max_class){
				// 如果可能取值数大于85%的样本数
				drop_column_mut += col_
			}else if(cached.agg("count"->"max").take(1).mkString.replace("[","").replace("]","").toInt > max_class){
				// 如果单个取值可能性大于85%以上
				drop_column_mut += col_
			}else{
				// 如果不触发剔除条件:将分类变量的取值占比排序,最多取前85%的分类,占比小于5%及其余的分类记为"zzz_others"

				// 将分类变量的取值占比排序,最多取前85%的分类
				cached = cached.withColumn("perc", col("count") / total_n )  // 筛选占比在指定值以下的分类
				cached = cached.withColumn("cumperc", sum("perc").over(Window.orderBy(desc("perc"))))  // 计算累积占比
				val keep_dumm = cached.filter(col("cumperc") <= min_cumperc)
					.select(col_).rdd.collect( ).map(row => row.mkString) // 获得累积在给定前85%的变量
				val keep_big_dumm = cached.filter(col("perc") >= min_class_perc)
					.select(col_).rdd.collect( ).map(row => row.mkString) // 获得占比大于5%的变量

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
			cached.unpersist() // 取消缓存
		}


		// 剔除上述不合适的列,得到选择后的数据
		val drop_column_list:Array[String] = drop_column_mut.toArray.distinct  // 去重
		val SelectFeature = FeatureVar diff drop_column_list  // 选择剔除后的变量
		val select_class_var = ClassVar diff drop_column_list  // 选择剔除后的分类变量
		val select_column = SelectFeature ++ Array(TargetVar)   // 后续需要选择的列
		val Micro_pick_data = Micro_dum_data.select(select_column.toSeq.map(Micro_dum_data.col(_)): _*)
		println("[INFO] 经过第一步筛选后共剩下  "+Micro_pick_data.columns.length+"  个变量!")

		// 查看此时各个变量的取值可能性
		// val ValueCount:Array[Long] = select_class_var.map (col_ => Micro_pick_data.groupBy(col(col_)).count.count)
		for(col_ <- select_class_var){
			println("变量  "+col_ +"   有"+Micro_pick_data.groupBy(col(col_)).count.count+" 种可能取值")
		}

		// 剔除包含缺失值的行(any)
		val Micro_nonull_data = Micro_pick_data.na.drop()
		CheckCount(Micro_nonull_data)

		// 将清洗后的数据写入parquet
		Micro_nonull_data.write.parquet(OutputHDFS)
		println("[INFO] 数据清洗完成!")
	}

	// 计算数据集行列
	def CheckCount(df: DataFrame): Unit = {
		println("[INFO] 数据集中包含  "+df.count()+"  行 与   "+df.columns.length+"  列!")
	}

	// 计算缺失比例
	def CountNA(df: DataFrame): Map[String, Double] = {
		// 对于数据框中每一列计算缺失比例,并返回一个map
		val total_n = df.count()
		var var_na_ratio: Map[String, Double] = Map()
		for(col_ <- df.columns){
			var filter_cond: String = col_ + " is null"
			var_na_ratio += ( col_ -> (df.select(col_).filter(filter_cond).count/total_n.toDouble).formatted("%.4f").toDouble)
		}
		return var_na_ratio
	}
}
