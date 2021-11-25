// Databricks notebook source
// MAGIC %md ##Payment Types
// MAGIC 
// MAGIC Extract and transform Payment Types data.
// MAGIC 
// MAGIC Create dimension, load as an unmanaged table, as well as register as a global temp view.

// COMMAND ----------

var paymentTypesDF = spark
    .read
    .json("/mnt/datalake/PaymentTypes.json")

println("Extracted Payment Types data")

// COMMAND ----------

paymentTypesDF.createOrReplaceGlobalTempView("DimPaymentTypes")

println("Saved Payment Types dimension as a global temp view")

// COMMAND ----------

// Store the DataFrame as an Unmanaged Table
paymentTypesDF
    .write
    .mode(SaveMode.Overwrite)
    .option("path", "/mnt/datalake/DimensionalModel/Dimensions/PaymentTypesDimension.parquet")    
    .saveAsTable("TaxiServiceWarehouse.DimPaymentTypes") 

println("Saved Payment Types dataframe as a dimension and unmanaged table")

// COMMAND ----------

dbutils.notebook.exit("Success")
