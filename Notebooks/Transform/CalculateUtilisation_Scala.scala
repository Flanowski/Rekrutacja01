// Databricks notebook source
sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
//dbutils.fs.rm("dbfs:/user/hive/warehouse/destdata.db/test_utilization", recurse=true)

// COMMAND ----------

import org.apache.spark.sql.functions.{to_date, when, avg, row_number}
import org.apache.spark.sql.expressions.Window

var dfTest = spark.sql("SELECT * FROM srcData.test")
var dfClass = spark.sql("SELECT * FROM srcData.class")

dfTest = dfTest.where(!$"authorized_at".isNull)

var dfUtilisation = dfTest.join(dfClass, dfClass("id") === dfTest("class_id"), "inner")
                      .select($"class_id"
                              ,$"name".as("class_name")
                              ,$"teaching_hours" 
                              ,$"test.id".as("test_id")
                              ,$"test_level_id".as("test_level")
                              ,to_date($"test.created_at", "dd.MM.yy").as("test_created_at")
                              ,to_date($"test.authorized_at", "dd.MM.yy").as("test_authorized_at")
                              ,row_number().over(Window.partitionBy($"class_id").orderBy($"test.authorized_at")).as("class_test_number")
                             )
println(dfUtilisation.count())

// COMMAND ----------

dfUtilisation.write.format("DELTA")
  .option("mergeSchema", "true")
  .option("overwriteSchema", "true")
  .mode("overwrite")
  .saveAsTable("destData.test_utilization")