// Databricks notebook source
// MAGIC %md ##FHV Bases
// MAGIC 
// MAGIC Extract and transform FHV Bases data.
// MAGIC 
// MAGIC Create dimension, load as an unmanaged table, as well as register as a global temp view.

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

// COMMAND ----------

// Flatten out structure
var fhvBasesFlatDF = fhvBasesDF
                        .select(
                                  $"License Number".alias("BaseLicenseNumber"),
                                  $"Entity Name".alias("EntityName"),
                                  $"Telephone Number".alias("TelephoneNumber"),
                                  $"SHL Endorsed".alias("ShlEndorsed"),
                                  $"Type of Base".alias("BaseType"),

                                  $"Address.Building".alias("AddressBuilding"),
                                  $"Address.Street".alias("AddressStreet"),
                                  $"Address.City".alias("AddressCity"),
                                  $"Address.State".alias("AddressState"),
                                  $"Address.Postcode".alias("AddressPostCode"),
                          
                                  $"GeoLocation.Latitude".alias("GeoLocationLatitude"),
                                  $"GeoLocation.Longitude".alias("GeoLocationLongitude"),
                                  $"GeoLocation.Location".alias("GeoLocationLocation")
                               )

println("Extracted FHV Bases data")

// COMMAND ----------

fhvBasesFlatDF.createOrReplaceGlobalTempView("DimFHVBases")

println("Saved FHV Bases dimension as a global temp view")

// COMMAND ----------

// Store the DataFrame as an Unmanaged Table
fhvBasesFlatDF
    .write
    .mode(SaveMode.Overwrite)
    .option("path", "/mnt/datalake/DimensionalModel/Dimensions/FHVBasesDimension.parquet")    
    .saveAsTable("TaxiServiceWarehouse.DimFHVBases") 

println("Saved FHVBases dataframe as a dimension and unmanaged table")

// COMMAND ----------

dbutils.notebook.exit("Success")
