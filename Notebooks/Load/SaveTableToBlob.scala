// Databricks notebook source
dbutils.widgets.text("npTableName", "")

// COMMAND ----------

val storageAccountName = "blobrek01"
val storageAccountKey = "STXiovH7Z6anlO1jroajLPloY9zpwV/sPUCbdAT6EtThhU41PTE7eblCh4B0De2RBANzSSS3XAag2wEPP9FGOw=="
val containerName = "dest"
val tableName = dbutils.widgets.get("npTableName")

val path = "wasbs://%s@%s.blob.core.windows.net/%s".format(containerName, storageAccountName, tableName)

spark.conf.set("fs.azure.account.key.%s.blob.core.windows.net".format(storageAccountName), storageAccountKey)

val df = spark.sql("SELECT * FROM destdata.%s".format(tableName))

df.coalesce(1)
  .write
  .format("com.databricks.spark.csv")
  .option("header", true)
  .option("header", true)
  .mode(SaveMode.Overwrite)
  .save(path)