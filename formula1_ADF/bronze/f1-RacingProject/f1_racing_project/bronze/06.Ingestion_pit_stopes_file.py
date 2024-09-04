# Databricks notebook source
# MAGIC %md
# MAGIC ## ingestion of pit_stoprs_file

# COMMAND ----------

# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

file_name = dbutils.widgets.text("file_name","")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

df = spark.read.json(f"/mnt/sagen2databricks/bronze/{file_name}.json",multiLine=True,schema=pit_stops_schema)

# COMMAND ----------

rename_df = df.withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("raceId", "race_id")
                        

# COMMAND ----------

output_df = add_column(rename_df)

# COMMAND ----------

output_df.write.mode("overwrite").parquet(f"/mnt/sagen2databricks/silver/{file_name}")
display(output_df)
