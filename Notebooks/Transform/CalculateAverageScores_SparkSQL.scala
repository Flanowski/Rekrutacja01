// Databricks notebook source
sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
//dbutils.fs.rm("dbfs:/user/hive/warehouse/destdata.db/test_average_scores", recurse=true)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS  destData.test_average_scores
// MAGIC (
// MAGIC     class_id INT,
// MAGIC     class_name  STRING,
// MAGIC     teaching_hours  STRING,
// MAGIC     test_created_at  STRING,
// MAGIC     test_authorized_at  STRING,
// MAGIC     score_sum DECIMAL(16, 2)
// MAGIC ) USING DELTA; 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC WITH _data AS
// MAGIC (
// MAGIC   SELECT 
// MAGIC     class_id,
// MAGIC     Class.name AS class_name,
// MAGIC     teaching_hours,
// MAGIC     to_date(Test.created_at, "dd.MM.yy") AS test_created_at,
// MAGIC     to_date(Test.authorized_at, "dd.MM.yy") AS test_authorized_at,
// MAGIC     ifnull(speaking_score, 0) + ifnull(writing_score, 0) + ifnull(reading_score, 0) + ifnull(listening_score, 0) AS score_sum
// MAGIC   FROM srcData.test Test
// MAGIC   INNER JOIN srcData.class Class
// MAGIC     ON Test.class_id = Class.id
// MAGIC   WHERE Test.test_status = 'SCORING_SCORED' 
// MAGIC     AND Test.authorized_at IS NOT NULL
// MAGIC )
// MAGIC INSERT OVERWRITE destData.test_average_scores
// MAGIC SELECT 
// MAGIC   class_id,
// MAGIC   class_name,
// MAGIC   teaching_hours,
// MAGIC   test_created_at,
// MAGIC   test_authorized_at,
// MAGIC   AVG(score_sum) AS avg_class_test_overall_score
// MAGIC FROM _data
// MAGIC GROUP BY  
// MAGIC   class_id,
// MAGIC   class_name,
// MAGIC   teaching_hours,
// MAGIC   test_created_at,
// MAGIC   test_authorized_at