// Databricks notebook source
// MAGIC %md ##FHV Taxi Data
// MAGIC 
// MAGIC Extract, clean, transform and load FHV Taxi trip data for a month.
// MAGIC 
// MAGIC Create fact, load as an unmanaged table, as well as register as a global temp view.

// COMMAND ----------

dbutils.widgets.text("ProcessMonth", "201901", "Process Month (yyyymm)")

// COMMAND ----------

var processMonth = dbutils.widgets.get("ProcessMonth")

// COMMAND ----------

// Define schema for FHV taxi trip file
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

val fhvTaxiTripSchema = StructType(
                    List(
                          StructField("Pickup_DateTime", TimestampType, true),
                          StructField("DropOff_datetime", TimestampType, true),
                          StructField("PUlocationID", IntegerType, true),
                          StructField("DOlocationID", IntegerType, true),
                          StructField("SR_Flag", IntegerType, true),
                          StructField("Dispatching_base_number", StringType, true),
                          StructField("Dispatching_base_num", StringType, true)
                    )
             )

println("Defined schema for FHV Taxi data")

// COMMAND ----------

println("Starting to extract FHV Taxi data from multiple files")

// Extract data from multiple FHV files
var fhvTaxiTripDataDF = spark
    .read
    .option("header", "true")
    .schema(fhvTaxiTripSchema)
    .csv(s"/mnt/storage/FHVTaxiTripData_{$processMonth}_*.csv")

println("Extracted FHV Taxi data")

// COMMAND ----------

println("Starting cleanup and transformation on FHV Taxi data")

import org.apache.spark.sql.functions._

// Apply transformations to FHV trip data
fhvTaxiTripDataDF = fhvTaxiTripDataDF

                        // Clean and filter the data
                        .na.drop(Seq("PULocationID", "DOLocationID"))
                        .dropDuplicates()                        

                        // Select only limited columns
                        .select(
                                  $"Pickup_DateTime".alias("PickupTime"),                          
                                  $"DropOff_DateTime", 
                                  $"PUlocationID", 
                                  $"DOlocationID", 
                                  $"SR_Flag", 
                                  $"Dispatching_base_number"
                               )

                        // Rename the columns
                        .withColumnRenamed("DropOff_DateTime", "DropTime")
                        .withColumnRenamed("PUlocationID", "PickupLocationId")
                        .withColumnRenamed("DOlocationID", "DropLocationId")
                        .withColumnRenamed("Dispatching_base_number", "BaseLicenseNumber")

                        // Create derived columns for year, month and day
                        .withColumn("TripYear", year($"PickupTime"))
                        .withColumn("TripMonth", month($"PickupTime"))
                        .select(
                                  $"*", 
                                  dayofmonth($"PickupTime").alias("TripDay")
                               )

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
                                                $"SR_Flag" === 1,
                                                  "SharedTrip"
                                            )
                                        .otherwise("SoloTrip")
                                   )
                        .drop("SR_Flag")

println("Cleaned up and applied transformations on FHV Taxi data")

// COMMAND ----------

fhvTaxiTripDataDF.createOrReplaceGlobalTempView("FactFhvTaxiTripData")

println("Saved FHV Taxi fact as a global temp view")

// COMMAND ----------

println("Starting to save FHV Taxi dataframe as a fact and unmanaged table")

// Store the DataFrame as an Unmanaged Table
fhvTaxiTripDataDF
    .write
    .mode(SaveMode.Overwrite)
    .option("path", "/mnt/datalake/DimensionalModel/Facts/FhvTaxiFact.parquet")    
    .saveAsTable("TaxiServiceWarehouse.FactFhvTaxiTripData") 

println("Saved FHV Taxi dataframe as a fact and unmanaged table")

// COMMAND ----------

dbutils.notebook.exit("Success")
