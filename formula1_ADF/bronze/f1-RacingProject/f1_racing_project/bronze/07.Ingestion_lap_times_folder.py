# Databricks notebook source
# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

file_name = dbutils.widgets.text("file_name","")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

df = spark.read .csv(f"/mnt/sagen2databricks/bronze/{file_name}",schema=lap_times_schema)
display(df)

# COMMAND ----------

rename_df = df.withColumnRenamed("raceId","race_Id")\
                .withColumnRenamed("driverId","driver_Id")

# COMMAND ----------

output_df = add_column(rename_df)

# COMMAND ----------

output_df.write.mode("overwrite").parquet(f"/mnt/sagen2databricks/silver/{file_name}")
display(output_df)
