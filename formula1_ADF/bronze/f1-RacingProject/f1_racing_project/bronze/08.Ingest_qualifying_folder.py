# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestion of qualifying folder containing multiple json files

# COMMAND ----------

# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

file_name = dbutils.widgets.text("file_name","")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

df = spark.read .json(f"/mnt/sagen2databricks/bronze/{file_name}",multiLine=True,schema=qualifying_schema)


# COMMAND ----------

rename_df = df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumnRenamed("q1","qualifying1")\
                        .withColumnRenamed("q2","qualifying2")\
                        .withColumnRenamed("q3","qualifying3")

# COMMAND ----------

output_df = add_column(rename_df)


# COMMAND ----------

output_df.write.mode("overwrite").parquet(f"/mnt/sagen2databricks/silver/{file_name}")
display(output_df)
