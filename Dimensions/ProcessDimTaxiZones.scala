// Databricks notebook source
// MAGIC %md ##Taxi Zones
// MAGIC 
// MAGIC Extract and transform Taxi Zones data.
// MAGIC 
// MAGIC Create dimension, load as an unmanaged table, as well as register as a global temp view.

// COMMAND ----------

var taxiZonesDF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/mnt/datalake/TaxiZones.csv")

println("Extracted Taxi Zones data")

// COMMAND ----------

taxiZonesDF.createOrReplaceGlobalTempView("DimTaxiZones")

println("Saved Taxi Zones dimension as a global temp view")

// COMMAND ----------

// Store the DataFrame as an Unmanaged Table
taxiZonesDF
    .write
    .mode(SaveMode.Overwrite)
    .option("path", "/mnt/datalake/DimensionalModel/Dimensions/TaxiZonesDimension.parquet")    
    .saveAsTable("TaxiServiceWarehouse.DimTaxiZones") 

println("Saved Taxi Zones dataframe as a dimension and unmanaged table")

// COMMAND ----------

dbutils.notebook.exit("Success")

// COMMAND ----------


