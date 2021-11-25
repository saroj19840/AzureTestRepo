// Databricks notebook source
// MAGIC %md ###Section 1: Exploration Operations
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.

// COMMAND ----------

display(
  dbutils.fs.ls("/mnt/datalake")
)

// COMMAND ----------

// MAGIC %fs ls /mnt/datalake

// COMMAND ----------

// MAGIC %fs head /mnt/datalake/YellowTaxiTripData_201812.csv

// COMMAND ----------

// MAGIC %md ###Section 2: Yellow Taxi Operations
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.
// MAGIC 
// MAGIC You can either execute individual commands, or can execute all commands at once by chaining them (mentioned below).

// COMMAND ----------

var yellowTaxiTripDataDF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/mnt/datalake/YellowTaxiTripData_201812.csv")

// COMMAND ----------

display(
    yellowTaxiTripDataDF.describe(
                                     "passenger_count",                                     
                                     "trip_distance"                                     
                                 )
)

// COMMAND ----------

// Display the count before filtering
println("Before filter = " + yellowTaxiTripDataDF.count())

// Filter inaccurate data
yellowTaxiTripDataDF = yellowTaxiTripDataDF
                          .where("passenger_count > 0")
                          .filter($"trip_distance" > 0.0)

// Display the count after filtering
println("After filter = " + yellowTaxiTripDataDF.count())

// COMMAND ----------

// Display the count before filtering
println("Before filter = " + yellowTaxiTripDataDF.count())

// Drop rows with nulls in PULocationID or DOLocationID
yellowTaxiTripDataDF = yellowTaxiTripDataDF
                          .na.drop(
                                    Seq("PULocationID", "DOLocationID")
                                  )

// Display the count after filtering
println("After filter = " + yellowTaxiTripDataDF.count())

// COMMAND ----------

// Map of default values
val defaultValueMap = Map(
                            "payment_type" -> 5,
                            "RatecodeID" -> 1
                         )

// Replace nulls with default values
yellowTaxiTripDataDF = yellowTaxiTripDataDF
                          .na.fill(defaultValueMap)

// COMMAND ----------

// Display the count before filtering
println("Before filter = " + yellowTaxiTripDataDF.count())

// Drop duplicate rows
yellowTaxiTripDataDF = yellowTaxiTripDataDF
                          .dropDuplicates()

// Display the count after filtering
println("After filter = " + yellowTaxiTripDataDF.count())

// COMMAND ----------

// Display the count before filtering
println("Before filter = " + yellowTaxiTripDataDF.count())

// Drop duplicate rows
yellowTaxiTripDataDF = yellowTaxiTripDataDF
                          .where("tpep_pickup_datetime >= '2018-12-01' AND tpep_dropoff_datetime < '2019-01-01'")

// Display the count after filtering
println("After filter = " + yellowTaxiTripDataDF.count())

// COMMAND ----------

// DBTITLE 1,All Yellow Taxi clean up operations chained together
val defaultValueMap = Map(
                            "payment_type" -> 5,
                            "RatecodeID" -> 1
                         )

var yellowTaxiTripDataDF = spark
                              .read
                              .option("header", "true")
                              .option("inferSchema", "true")
                              .csv("/mnt/datalake/YellowTaxiTripData_201812.csv")

yellowTaxiTripDataDF = yellowTaxiTripDataDF
                              .where("passenger_count > 0")
                              .filter($"trip_distance" > 0.0)

                              .na.drop(Seq("PULocationID", "DOLocationID"))

                              .na.fill(defaultValueMap)

                              .dropDuplicates()

                              .where("tpep_pickup_datetime >= '2018-12-01' AND tpep_dropoff_datetime < '2019-01-01'")

// Display the count after filtering
println("After filters = " + yellowTaxiTripDataDF.count())

// COMMAND ----------

// DBTITLE 1,All Yellow Taxi transformations chained together
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

// COMMAND ----------

// MAGIC %md ###Section 3: Green Taxi Operations
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.
// MAGIC 
// MAGIC You can either execute individual commands, or can execute all commands at once by chaining them (mentioned below).

// COMMAND ----------

var greenTaxiTripDataDF = spark
    .read
    .option("header", "true")
    .option("delimiter", "\t")    
    .csv("/mnt/datalake/GreenTaxiTripData_201812.csv")

// COMMAND ----------

// MAGIC %fs head /mnt/datalake/PaymentTypes.json

// COMMAND ----------

var paymentTypesDF = spark
    .read
    .json("/mnt/datalake/PaymentTypes.json")

display(paymentTypesDF)

// COMMAND ----------

// DBTITLE 1,All Green Taxi clean up operations chained together
val defaultValueMap = Map(
                            "payment_type" -> 5,
                            "RatecodeID" -> 1
                         )

var greenTaxiTripDataDF = spark
                              .read
                              .option("header", "true")
                              .option("inferSchema", "true")                              
                              .option("delimiter", "\t")    
                              .csv("/mnt/datalake/GreenTaxiTripData_201812.csv")

greenTaxiTripDataDF = greenTaxiTripDataDF
                              .where("passenger_count > 0")
                              .filter($"trip_distance" > 0.0)

                              .na.drop(Seq("PULocationID", "DOLocationID"))

                              .na.fill(defaultValueMap)

                              .dropDuplicates()

                              .where("lpep_pickup_datetime >= '2018-12-01' AND lpep_dropoff_datetime < '2019-01-01'")

// Display the count after filtering
println("After filters = " + greenTaxiTripDataDF.count())

// COMMAND ----------

// DBTITLE 1,All Green Taxi transformations chained together
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

// Display the count after filtering
println("After filters = " + greenTaxiTripDataDF.count())

// COMMAND ----------

// MAGIC %md ###Section 4: Create Temp Views for Yellow and Green Taxi Data
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.

// COMMAND ----------

yellowTaxiTripDataDF.createOrReplaceGlobalTempView("FactYellowTaxiTripData")

// COMMAND ----------

greenTaxiTripDataDF.createOrReplaceGlobalTempView("FactGreenTaxiTripData")

// COMMAND ----------

// MAGIC %md ###Section 5: Loading Yellow and Green Taxi Data
// MAGIC 
// MAGIC Please note that this contains complete code. As we write the code with each module, some changes are done to commands or additional commands are added. Please refer to video while executing commands.

// COMMAND ----------

// Load the dataframe as CSV to data lake
greenTaxiTripDataDF  
    .write
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    .mode(SaveMode.Overwrite)
    .csv("/mnt/datalake/DimensionalModel/Facts/GreenTaxiFact.csv")

// COMMAND ----------

// Check the number of default partitions in the config
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

// Set the number of default partitions in the config
spark.conf.set("spark.sql.shuffle.partitions", 10)

// COMMAND ----------

// Check the number of partitions on the dataframe
greenTaxiTripDataDF
    .rdd
    .getNumPartitions

// COMMAND ----------

// Decrease the number of partitions on dataframe to 1
greenTaxiTripDataDF = greenTaxiTripDataDF  
                        .coalesce(1)

// Check the number of partitions on the dataframe
greenTaxiTripDataDF
    .rdd
    .getNumPartitions

// COMMAND ----------

// Load the dataframe as CSV to data lake, now as a single partition
greenTaxiTripDataDF
    .write
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    .mode(SaveMode.Overwrite)     
    .csv("/mnt/datalake/DimensionalModel/Facts/GreenTaxiFact.csv")

// COMMAND ----------

// Load the dataframe as parquet to data lake
greenTaxiTripDataDF      
    .write
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    .mode(SaveMode.Overwrite)    
    .parquet("/mnt/datalake/DimensionalModel/Facts/GreenTaxiFact.parquet")

// COMMAND ----------

// Read the stored CSV file
val greenTaxiCsvDF = spark
                        .read
                        .option("header", "true")
                        .csv("/mnt/datalake/DimensionalModel/Facts/GreenTaxiFact.csv")                        

greenTaxiCsvDF.select("PickupLocationId", "DropLocationId").distinct().count()

// COMMAND ----------

// Read the stored Parquet file
val greenTaxiParquetDF = spark
                            .read                            
                            .parquet("/mnt/datalake/DimensionalModel/Facts/GreenTaxiFact.parquet")                            

greenTaxiParquetDF.select("PickupLocationId", "DropLocationId").distinct().count()

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS TaxiServiceWarehouse

// COMMAND ----------

// Store the DataFrame as a Managed Table
greenTaxiTripDataDF
    .write
    .mode(SaveMode.Overwrite)
    .saveAsTable("TaxiServiceWarehouse.FactGreenTaxiTripDataManaged")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT *
// MAGIC FROM TaxiServiceWarehouse.FactGreenTaxiTripDataManaged
// MAGIC LIMIT 10

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DESCRIBE EXTENDED TaxiServiceWarehouse.FactGreenTaxiTripDataManaged

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP TABLE TaxiServiceWarehouse.FactGreenTaxiTripDataManaged

// COMMAND ----------

// Store the DataFrame as an Unmanaged Table
greenTaxiTripDataDF
    .write
    .mode(SaveMode.Overwrite)
    .option("path", "/mnt/datalake/DimensionalModel/Facts/GreenTaxiFact.parquet")
    //.format("csv")   /* Default format is parquet */
    .saveAsTable("TaxiServiceWarehouse.FactGreenTaxiTripData") 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DESCRIBE EXTENDED TaxiServiceWarehouse.FactGreenTaxiTripData

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP TABLE TaxiServiceWarehouse.FactGreenTaxiTripData

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS TaxiServiceWarehouse.FactGreenTaxiTripData
// MAGIC     USING parquet
// MAGIC     OPTIONS 
// MAGIC     (
// MAGIC         path "/mnt/datalake/DimensionalModel/Facts/GreenTaxiFact.parquet"
// MAGIC     )

// COMMAND ----------


