// Databricks notebook source
dbutils.widgets.text("npTableName", "")

// COMMAND ----------

val storageAccountName = "blobrek01"
val storageAccountKey = "STXiovH7Z6anlO1jroajLPloY9zpwV/sPUCbdAT6EtThhU41PTE7eblCh4B0De2RBANzSSS3XAag2wEPP9FGOw=="
val containerName = "temp"
val tableName = dbutils.widgets.get("npTableName")
val tempDir = "wasbs://%s@%s.blob.core.windows.net/%s".format(containerName, storageAccountName, tableName)

spark.conf.set("fs.azure.account.key.%s.blob.core.windows.net".format(storageAccountName), storageAccountKey)

dbutils.fs.rm(tempDir, true)

// COMMAND ----------

val url = "sqlsrvrek01.database.windows.net"
val user = "AdminRekrutacja"
val pass = "Rekrutacja123"

val url2 = s"jdbc:sqlserver://sqlsrvrek01.database.windows.net;database=sqldbrek01"

val events = spark.table("destdata.%s".format(tableName))
  .write
  .format("jdbc")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("url", url2)
  .option("user", user)
  .option("password", pass)
  .option("tempDir", tempDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "rek.%s".format(tableName))
  .mode(SaveMode.Overwrite)
  .save()