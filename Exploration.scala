// Databricks notebook source
// MAGIC %md ### Exploration Notebook
// MAGIC 
// MAGIC testing new commands

// COMMAND ----------

"Hello World"

// COMMAND ----------

var sum = 1 + 1

// COMMAND ----------

sum = sum + 1

sum = sum + 2

// COMMAND ----------

1+1

// COMMAND ----------

display(
  dbutils.fs.ls("/mnt/datalake")
)

// COMMAND ----------

dbutils.fs.head("/mnt/datalake/TaxiZones.csv")

// COMMAND ----------


