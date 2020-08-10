// Databricks notebook source
dbutils.widgets.text("npFileName", "")

// COMMAND ----------

val storageAccountName = "blobrek01"
val storageAccountKey = "STXiovH7Z6anlO1jroajLPloY9zpwV/sPUCbdAT6EtThhU41PTE7eblCh4B0De2RBANzSSS3XAag2wEPP9FGOw=="
val containerName = "source"
val fileName = dbutils.widgets.get("npFileName")

val path = "wasbs://%s@%s.blob.core.windows.net/%s.csv".format(containerName, storageAccountName, fileName)

spark.conf.set("fs.azure.account.key.%s.blob.core.windows.net".format(storageAccountName), storageAccountKey)

val df = 
  spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("sep", ";")
  .option("mode", "DROPMALFORMED")
  .load(path)

df.write.format("DELTA")
  .option("mergeSchema", "true")
  .option("overwriteSchema", "true")
  .mode("overwrite")
  .saveAsTable("srcData.%s".format(fileName))