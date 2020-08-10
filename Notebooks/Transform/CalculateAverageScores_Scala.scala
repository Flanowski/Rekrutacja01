// Databricks notebook source
sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
//dbutils.fs.rm("dbfs:/user/hive/warehouse/destdata.db/test_average_scores", recurse=true)

// COMMAND ----------

import org.apache.spark.sql.functions.{to_date, when, avg}

var dfTest = spark.sql("SELECT * FROM srcData.test")
var dfClass = spark.sql("SELECT * FROM srcData.class")

dfTest = dfTest.where($"test_status" === "SCORING_SCORED" && !$"authorized_at".isNull)
var dfAverageScores = dfTest.join(dfClass, dfClass("id") === dfTest("class_id"), "inner")
                      .select($"class_id"
                              ,$"name".as("class_name")
                              ,$"teaching_hours" 
                              ,to_date($"test.created_at", "dd.MM.yy").as("test_created_at")
                              ,to_date($"test.authorized_at", "dd.MM.yy").as("test_authorized_at")
                              ,(when($"speaking_score".isNull, 0).otherwise($"speaking_score") 
                                + when($"writing_score".isNull, 0).otherwise($"writing_score") 
                                + when($"reading_score".isNull, 0).otherwise($"reading_score")
                                + when($"listening_score".isNull, 0).otherwise($"listening_score")).as("score_sum")
                             )
                      .groupBy("class_id", "class_name", "teaching_hours", "test_created_at", "test_authorized_at")
                      .agg(avg("score_sum").as("avg_class_test_overall_score"))

// COMMAND ----------

dfAverageScores.write.format("DELTA")
  .option("mergeSchema", "true")
  .option("overwriteSchema", "true")
  .mode("overwrite")
  .saveAsTable("destData.test_average_scores")