# Databricks notebook source
# MAGIC %md
# MAGIC ###ingestion circuite file
# MAGIC

# COMMAND ----------

file_name=dbutils.widgets.text("file_name",'')
file_name=dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

schema_circuite = StructType(fields=[StructField("circuitId",IntegerType()),
                                    StructField("circuitRef",StringType()),
                                    StructField("name",StringType()),
                                    StructField("location",StringType()),
                                    StructField("country",StringType()),
                                    StructField("lat",FloatType()),
                                    StructField("lng",FloatType()),
                                    StructField("alt",IntegerType()),
                                    StructField("url",StringType())

])

# COMMAND ----------

circuit_df = spark.read.csv(f"/mnt/sagen2databricks/bronze/{file_name}.csv", header=True, schema=schema_circuite)


# COMMAND ----------

output_df = add_column(circuit_df)


# COMMAND ----------

rename_df = output_df.withColumnRenamed("circuitId","circuit_Id")\
                       .withColumnRenamed("circuitRef","circuit_Ref")\
                       .withColumnRenamed("name","circuit_name")\
                       .withColumnRenamed("location","circuit_location")\
                       .withColumnRenamed("country","circuit_country")\
                       .withColumnRenamed("lat","circuit-lat")\
                       .withColumnRenamed("lng","circuit-lng")\
                       .withColumnRenamed("alt","circuit_alt")\
                       .withColumnRenamed("url","circuit_url")

# COMMAND ----------

rename_df.write.mode("overwrite").parquet(f"/mnt/sagen2databricks/silver/{file_name}")
