// Databricks notebook source
// MAGIC %md ##Yellow Taxi Data
// MAGIC 
// MAGIC Extract, clean, transform and load Yellow Taxi trip data for a month.
// MAGIC 
// MAGIC Create fact, load as an unmanaged table, as well as register as a global temp view.

// COMMAND ----------

dbutils.widgets.text("ProcessMonth", "201901", "Process Month (yyyymm)")

// COMMAND ----------

var processMonth = dbutils.widgets.get("ProcessMonth")

// COMMAND ----------

println("Starting to extract Yellow Taxi data")

// Extract and clean Yellow Taxi Data
val defaultValueMapForYellowTaxi = Map(
                                        "payment_type" -> 5,
                                        "RatecodeID" -> 1
                                      )

// Extract Yellow Taxi file
var yellowTaxiTripDataDF = spark
                              .read
                              .option("header", "true")
                              .option("inferSchema", "true")
                              .csv(s"/mnt/datalake/YellowTaxiTripData_$processMonth.csv")

// Clean Yellow Taxi data
yellowTaxiTripDataDF = yellowTaxiTripDataDF
                              .where("passenger_count > 0")
                              .filter($"trip_distance" > 0.0)

                              .na.drop(Seq("PULocationID", "DOLocationID"))

                              .na.fill(defaultValueMapForYellowTaxi)

                              .dropDuplicates()                              

println("Extracted and cleaned Yellow Taxi data")

// COMMAND ----------

println("Starting transformation on Yellow Taxi data")

import org.apache.spark.sql.functions._

// Apply transformations to Yellow taxi data
yellowTaxiTripDataDF = yellowTaxiTripDataDF

                        // Select only limited columns
                        .select(
                                  $"VendorID",
                                  $"passenger_count".alias("PassengerCount"),
                                  $"trip_distance".alias("TripDistance"),
                                  $"tpep_pickup_datetime".alias("PickupTime"),                          
                                  $"tpep_dropoff_datetime".alias("DropTime"), 
                                  $"PUlocationID".alias("PickupLocationId"), 
                                  $"DOlocationID".alias("DropLocationId"), 
                                  $"RatecodeID", 
                                  $"total_amount".alias("TotalAmount"),
                                  $"payment_type".alias("PaymentType")
                               )

                        // Create derived columns for year, month and day
                        .withColumn("TripYear", year($"PickupTime"))
                        .withColumn("TripMonth", month($"PickupTime"))
                        .withColumn("TripDay", dayofmonth($"PickupTime"))
                        
                        // Create a derived column - Trip time in minutes
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp($"DropTime") - unix_timestamp($"PickupTime")) 
                                                    / 60
                                             )
                                   )

                        // Create a derived column - Trip type, and drop SR_Flag column
                        .withColumn("TripType", 
                                        when(
                                                $"RatecodeID" === 6,
                                                  "SharedTrip"
                                            )
                                        .otherwise("SoloTrip")
                                   )
                        .drop("RatecodeID")

println("Applied transformations on Yellow Taxi data")

// COMMAND ----------

yellowTaxiTripDataDF.createOrReplaceGlobalTempView("FactYellowTaxiTripData")

println("Saved Yellow Taxi fact as a global temp view")

// COMMAND ----------

println("Starting to save Yellow Taxi dataframe as a fact and unmanaged table")

// Store the DataFrame as an Unmanaged Table
yellowTaxiTripDataDF
    .write
    .mode(SaveMode.Append)
    .option("path", "/mnt/datalake/DimensionalModel/Facts/YellowTaxiFact.parquet")
    .saveAsTable("TaxiServiceWarehouse.FactYellowTaxiTripData") 

println("Saved Yellow Taxi dataframe as a fact and unmanaged table")

// COMMAND ----------

dbutils.notebook.exit("Success")

// COMMAND ----------


