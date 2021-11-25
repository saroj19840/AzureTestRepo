// Databricks notebook source
// MAGIC %md ###Section 1: FHV Taxi Operations
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.
// MAGIC 
// MAGIC You can either execute individual commands, or can execute all commands at once by chaining them (mentioned below).

// COMMAND ----------

var fhvTaxiTripDataDF_InferSchema = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/mnt/storage/FHVTaxiTripData_201812_01.csv")

// COMMAND ----------

// Create schema for FHV taxi data
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

// COMMAND ----------

// Read multiple files of FHV taxi data
var fhvTaxiTripDataMultipleFilesDF = spark
    .read    
    .schema(fhvTaxiTripSchema)
    .csv("/mnt/storage/FHVTaxiTripData_201812_*.csv")

// COMMAND ----------

// Apply schema to FHV taxi data
var fhvTaxiTripDataDF = spark
    .read
    .option("header", "true")
    .schema(fhvTaxiTripSchema)
    .csv("/mnt/storage/FHVTaxiTripData_201812_01.csv")

// COMMAND ----------

// Run describe on FHV taxi data
display(
  fhvTaxiTripDataDF.describe()
)

// COMMAND ----------

// Clean and filter the FHV taxi data
import org.apache.spark.sql.functions._

fhvTaxiTripDataDF = fhvTaxiTripDataDF
                        .na.drop(Seq("PULocationID", "DOLocationID"))
                        .dropDuplicates()
                        .where("Pickup_DateTime >= '2018-12-01' AND Dropoff_DateTime < '2019-01-01'")

// COMMAND ----------

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

// Select only limited columns
fhvTaxiTripDataDF = fhvTaxiTripDataDF
                        .select(
                                  "Pickup_DateTime", 
                                  "DropOff_DateTime", 
                                  "PUlocationID", 
                                  "DOlocationID", 
                                  "SR_Flag", 
                                  "Dispatching_base_number"
                               )                        

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

// Select and rename columns
fhvTaxiTripDataDF = fhvTaxiTripDataDF
                        .select(
                                  $"Pickup_DateTime".alias("PickupTime"), 
                          
                                  $"DropOff_DateTime", 
                                  $"PUlocationID", 
                                  $"DOlocationID", 
                                  $"SR_Flag", 
                                  $"Dispatching_base_number"
                               )

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

// Rename the columns
fhvTaxiTripDataDF = fhvTaxiTripDataDF
                        .withColumnRenamed("DropOff_DateTime", "DropTime")
                        .withColumnRenamed("PUlocationID", "PickupLocationId")
                        .withColumnRenamed("DOlocationID", "DropLocationId")
                        .withColumnRenamed("Dispatching_base_number", "BaseLicenseNumber")

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

// Create derived columns for year, month and day
fhvTaxiTripDataDF = fhvTaxiTripDataDF
                        .withColumn("TripYear", year($"PickupTime"))
                        .withColumn("TripMonth", month($"PickupTime"))

                        .select(
                                  $"*", 
                                  dayofmonth($"PickupTime").alias("TripDay")
                               )

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

// Create a derived column - Trip time in minutes
fhvTaxiTripDataDF = fhvTaxiTripDataDF
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp($"DropTime") - unix_timestamp($"PickupTime")) 
                                                    / 60
                                             )
                                   )                                               

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

// Create a derived column - Trip type, and drop SR_Flag column
fhvTaxiTripDataDF = fhvTaxiTripDataDF
                        .withColumn("TripType", 
                                        when(
                                                $"SR_Flag" === 1,
                                                  "SharedTrip"
                                            )
                                        .otherwise("SoloTrip")
                                   )
                        .drop("SR_Flag")

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

// MAGIC %md ###Section 2: FHV Bases Operations
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.
// MAGIC 
// MAGIC You can either execute individual commands, or can execute all commands at once by chaining them (mentioned below).

// COMMAND ----------

// MAGIC %fs head /mnt/storage/FhvBases.json

// COMMAND ----------

/// Read FHV Bases json file
var fhvBasesDF = spark
  .read
  .option("multiline", "true")
  .json("/mnt/storage/FhvBases.json")

display(fhvBasesDF)

// COMMAND ----------

// Create schema for FHV Bases
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

val fhvBasesSchema = StructType(
  List(
    StructField("License Number", StringType, true),
    StructField("Entity Name", StringType, true),
    StructField("Telephone Number", LongType, true),
    StructField("SHL Endorsed", StringType, true),
    StructField("Type of Base", StringType, true),
    
    StructField("Address", 
                StructType(List(
                    StructField("Building", StringType, true),
                    StructField("Street", StringType, true), 
                    StructField("City", StringType, true), 
                    StructField("State", StringType, true), 
                    StructField("Postcode", StringType, true))),
                true
               ),
                
    StructField("GeoLocation", 
                StructType(List(
                    StructField("Latitude", StringType, true),
                    StructField("Longitude", StringType, true), 
                    StructField("Location", StringType, true))),
                true
              )   
  )
)

// COMMAND ----------

// Apply schema to FHV bases
var fhvBasesDF = spark.read
  .schema(fhvBasesSchema)
  .option("multiline", "true")
  .json("/mnt/storage/FhvBases.json")

display(fhvBasesDF)

// COMMAND ----------

var fhvBasesFlatDF = fhvBasesDF
                        .select(
                                  $"License Number".alias("BaseLicenseNumber"),                          
                                  $"Type of Base".alias("BaseType"),

                                  $"Address.Building".alias("AddressBuilding"),
                                  $"Address.Street".alias("AddressStreet"),
                                  $"Address.City".alias("AddressCity"),
                                  $"Address.State".alias("AddressState"),
                                  $"Address.Postcode".alias("AddressPostCode")
                               )

display(fhvBasesFlatDF)

// COMMAND ----------

// MAGIC %md ###Section 3: Join FHV Taxi Data with FHV Bases, and build report
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.
// MAGIC 
// MAGIC You can either execute individual commands, or can execute all commands at once by chaining them (mentioned below).

// COMMAND ----------

// Create a dataframe joining FHV trip data with bases
var fhvTaxiTripDataWithBasesDF = fhvTaxiTripDataDF
                                     .join(fhvBasesFlatDF,                                               
                                               Seq("BaseLicenseNumber"),
                                              "inner"
                                          )

display(fhvTaxiTripDataWithBasesDF)

// COMMAND ----------

// Create a dataframe for report
var fhvTaxiTripReportDF = fhvTaxiTripDataWithBasesDF
                              .groupBy("AddressCity", "BaseType")
                              .agg(sum("TripTimeInMinutes"))

                              .withColumnRenamed("sum(TripTimeInMinutes)", "TotalTripTime")
                              .orderBy("AddressCity", "BaseType")

display(fhvTaxiTripReportDF)

// COMMAND ----------

// MAGIC %md ###Section 4: All Operations chained together
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.

// COMMAND ----------

import org.apache.spark.sql.functions._

// Apply schema and extract FHV trip data
var fhvTaxiTripDataDF = spark
    .read
    .option("header", "true")
    .schema(fhvTaxiTripSchema)
    .csv("/mnt/storage/FHVTaxiTripData_201812_01.csv")

// Apply transformations to FHV trip data
fhvTaxiTripDataDF = fhvTaxiTripDataDF

                        // Clean and filter the data
                        .na.drop(Seq("PULocationID", "DOLocationID"))
                        .dropDuplicates()
                        .where("Pickup_DateTime >= '2018-12-01' AND Dropoff_DateTime < '2019-01-01'")

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


// Create a dataframe joining FHV trip data with bases
var fhvTaxiTripDataWithBasesDF = fhvTaxiTripDataDF
                                     .join(fhvBasesFlatDF,                                               
                                               Seq("BaseLicenseNumber"),
                                              "inner"
                                          )


// Create a dataframe for report
var fhvTaxiTripReportDF = fhvTaxiTripDataWithBasesDF
                              .groupBy("AddressCity", "BaseType")
                              .agg(sum("TripTimeInMinutes"))                              
                              .withColumnRenamed("sum(TripTimeInMinutes)", "TotalTripTime")
                              .orderBy("AddressCity", "BaseType")

// COMMAND ----------

// MAGIC %md ###Section 5: Create Temp Views and use Spark SQL 
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.

// COMMAND ----------

fhvTaxiTripDataWithBasesDF.printSchema

// COMMAND ----------

// Create local temp view - that can be used only in this notebook
fhvTaxiTripDataWithBasesDF.createOrReplaceTempView("LocalFhvTaxiTripData")

// COMMAND ----------

// Use spark.sql to run a SQL query
var sqlBasedDF = spark.sql("SELECT * FROM LocalFhvTaxiTripData WHERE BaseLicenseNumber = 'B02510'")

display(sqlBasedDF.limit(10))

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * 
// MAGIC FROM LocalFhvTaxiTripData
// MAGIC WHERE BaseLicenseNumber = 'B02510'
// MAGIC LIMIT 10

// COMMAND ----------

// Create global temp view - that can be used across notebooks
fhvTaxiTripDataWithBasesDF.createOrReplaceGlobalTempView("FactFhvTaxiTripData")

// COMMAND ----------


