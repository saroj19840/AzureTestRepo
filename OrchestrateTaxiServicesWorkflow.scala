// Databricks notebook source
dbutils.widgets.text("ProcessMonth", "201812", "Process Month (yyyymm)")

var processMonth = dbutils.widgets.get("ProcessMonth")

// COMMAND ----------

/*
// Create a parameter mapping to be passed to the callee notebook
var parametersMap = Map(
                          "ProcessMonth" -> processMonth
                       )

// Invoke Yellow Taxi Fact notebook
var status = dbutils
                .notebook
                .run(
                        "Facts/ProcessFactYellowTaxiTripData", 
                        300, 
                        parametersMap
                    )

// Check if notebook ran successfully
if (status == "Success") 
{
  println("Yellow Taxi Fact has been processed successfully")
}
else 
{
  println("Error in processing Yellow Taxi Fact")
}

*/

// COMMAND ----------

// Create a parameter mapping to be passed to the callee notebook
var parametersMap = Map(
                          "ProcessMonth" -> processMonth
                       )

// Invoke all dimension notebooks
var status = dbutils.notebook.run("Dimensions/ProcessDimTaxiZones", 60, Map())
status = dbutils.notebook.run("Dimensions/ProcessDimRateCodes", 60, Map())
status = dbutils.notebook.run("Dimensions/ProcessDimPaymentTypes", 60, Map())
status = dbutils.notebook.run("Dimensions/ProcessDimBases", 60, Map())

// Invoke all fact notebooks
status = dbutils.notebook.run("Facts/ProcessFactYellowTaxiTripData", 300, parametersMap)
status = dbutils.notebook.run("Facts/ProcessFactGreenTaxiTripData", 300, parametersMap)
status = dbutils.notebook.run("Facts/ProcessFactFHVTaxiTripData", 900, parametersMap)

// COMMAND ----------


