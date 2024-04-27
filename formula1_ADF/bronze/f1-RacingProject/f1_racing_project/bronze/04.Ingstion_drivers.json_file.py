# Databricks notebook source
file_name = dbutils.widgets.text("file_name","")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name",StructType([
                                    StructField("forename",StringType(),True),
                                    StructField("surname",StringType(),True)
                                    ])),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read.json(f"/mnt/sagen2databricks/bronze/{file_name}.json",schema=drivers_schema)


# COMMAND ----------

rename_df = drivers_df.withColumn("forename",drivers_df["name"]["forename"]).withColumn("surname",drivers_df["name"]["surname"]).withColumnRenamed("driverId","driver_Id").withColumnRenamed("driverRef","driver_Ref").withColumnRenamed("nationality","driver_nationality").drop("name")


# COMMAND ----------

output_df = add_column(rename_df)


# COMMAND ----------

output_df.write.mode("overwrite").parquet(f"/mnt/sagen2databricks/silver/{file_name}")
display(output_df)
