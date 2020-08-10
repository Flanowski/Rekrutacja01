// Databricks notebook source
sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
//dbutils.fs.rm("dbfs:/user/hive/warehouse/destdata.db/test_utilization", recurse=true)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS destData.test_utilization
// MAGIC (
// MAGIC   class_id INT,
// MAGIC   class_name STRING,
// MAGIC   teaching_hours STRING,
// MAGIC   test_id INT,
// MAGIC   test_level INT,
// MAGIC   test_created_at DATE,
// MAGIC   test_authorized_at DATE,
// MAGIC   class_test_number INT
// MAGIC )
// MAGIC USING DELTA;

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT OVERWRITE destData.test_utilization
// MAGIC SELECT 
// MAGIC   class_id,
// MAGIC   Class.name AS class_name,
// MAGIC   teaching_hours,
// MAGIC   Test.id AS test_id,
// MAGIC   test_level_id AS test_level,
// MAGIC   to_date(Test.created_at, "dd.MM.yy") AS test_created_at,
// MAGIC   to_date(Test.authorized_at, "dd.MM.yy") AS test_authorized_at,
// MAGIC   ROW_NUMBER() OVER(PARTITION BY class_id ORDER BY Test.authorized_at ASC) AS class_test_number
// MAGIC FROM srcData.test Test
// MAGIC INNER JOIN srcData.class Class
// MAGIC   ON Test.class_id = Class.id
// MAGIC WHERE Test.authorized_at IS NOT NULL