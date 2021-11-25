// Databricks notebook source
// MAGIC %md ##Green Taxi Data
// MAGIC 
// MAGIC Extract, clean, transform and load Green Taxi trip data for a month.
// MAGIC 
// MAGIC Create fact, load as an unmanaged table, as well as register as a global temp view.

// COMMAND ----------

dbutils.widgets.text("ProcessMonth", "201901", "Process Month (yyyymm)")

// COMMAND ----------

var processMonth = dbutils.widgets.get("ProcessMonth")

// COMMAND ----------

println("Starting to extract Green Taxi data")

// Extract and clean Green Taxi Data
val defaultValueMapForGreenTaxi = Map(
                                        "payment_type" -> 5,
                                        "RatecodeID" -> 1
                                     )

var greenTaxiTripDataDF = spark
                              .read
                              .option("header", "true")
                              .option("inferSchema", "true")                              
                              .option("delimiter", "\t")    
                              .csv(s"/mnt/datalake/GreenTaxiTripData_$processMonth.csv")

greenTaxiTripDataDF = greenTaxiTripDataDF
                              .where("passenger_count > 0")
                              .filter($"trip_distance" > 0.0)

                              .na.drop(Seq("PULocationID", "DOLocationID"))

                              .na.fill(defaultValueMapForGreenTaxi)

                              .dropDuplicates()                              

println("Extracted and cleaned Green Taxi data")

// COMMAND ----------

println("Starting transformation on Green Taxi data")

import org.apache.spark.sql.functions._

// Apply transformations to Green taxi data
greenTaxiTripDataDF = greenTaxiTripDataDF

                        // Select only limited columns
                        .select(
                                  $"VendorID",
                                  $"passenger_count".alias("PassengerCount"),
                                  $"trip_distance".alias("TripDistance"),
                                  $"lpep_pickup_datetime".alias("PickupTime"),                          
                                  $"lpep_dropoff_datetime".alias("DropTime"), 
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

println("Applied transformations on Green Taxi data")

// COMMAND ----------

greenTaxiTripDataDF.createOrReplaceGlobalTempView("FactGreenTaxiTripData")

println("Saved Green Taxi fact as a global temp view")

// COMMAND ----------

println("Starting to save Green Taxi dataframe as a fact and unmanaged table")

// Store the DataFrame as an Unmanaged Table
greenTaxiTripDataDF
    .write
    .mode(SaveMode.Overwrite)
    .option("path", "/mnt/datalake/DimensionalModel/Facts/GreenTaxiFact.parquet")    
    .saveAsTable("TaxiServiceWarehouse.FactGreenTaxiTripData") 

println("Saved Green Taxi dataframe as a fact and unmanaged table")

// COMMAND ----------

dbutils.notebook.exit("Success")

// COMMAND ----------


