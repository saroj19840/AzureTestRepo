// Databricks notebook source
// MAGIC %md ##Rate Codes
// MAGIC 
// MAGIC Extract and transform Rate Codes data.
// MAGIC 
// MAGIC Create dimension, load as an unmanaged table, as well as register as a global temp view.

// COMMAND ----------

var rateCodesDF = spark
    .read
    .option("mode", "DropMalformed")
    .json("/mnt/datalake/RateCodes.json")

println("Extracted Rate Codes data")

// COMMAND ----------

rateCodesDF.createOrReplaceGlobalTempView("DimRateCodes")

println("Saved Rate Codes dimension as a global temp view")

// COMMAND ----------

// Store the DataFrame as an Unmanaged Table
rateCodesDF
    .write
    .mode(SaveMode.Overwrite)
    .option("path", "/mnt/datalake/DimensionalModel/Dimensions/RateCodesDimension.parquet")    
    .saveAsTable("TaxiServiceWarehouse.DimRateCodes") 

println("Saved Rate Codes dataframe as a dimension and unmanaged table")

// COMMAND ----------

dbutils.notebook.exit("Success")
