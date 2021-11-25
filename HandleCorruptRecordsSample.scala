// Databricks notebook source
// MAGIC %fs head /mnt/datalake/RateCodes.json

// COMMAND ----------

// Specify different modes
var rateCodesJsonDF_modes = spark
    .read
    .option("mode", "Permissive")
    .json("/mnt/datalake/RateCodes.json")

display(rateCodesJsonDF_modes)

// COMMAND ----------

// Add bad records path
var rateCodesJsonDF_badpath = spark.read        
    .option("badRecordsPath", "/mnt/datalake/JsonBadRecords/")
    .json("/mnt/datalake/RateCodes.json")

display(rateCodesJsonDF_badpath)

// COMMAND ----------

// Check the bad records file
var jsonBadPathDF = spark
    .read    
    .json("/mnt/datalake/JsonBadRecords/20191022T095259/bad_records/part-00000-03736601-1a5b-4387-8dd2-32aa5581beff")

display(jsonBadPathDF)

// COMMAND ----------

// MAGIC %fs head /mnt/datalake/RateCodes.csv

// COMMAND ----------

var rateCodesCsvDF_modes = spark
    .read
    .option("header", "true")
    .option("mode", "Permissive")    
    .csv("/mnt/datalake/RateCodes.csv")

display(rateCodesCsvDF_modes)

// COMMAND ----------


