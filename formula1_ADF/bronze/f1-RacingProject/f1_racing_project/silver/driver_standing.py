# Databricks notebook source
from pyspark.sql.functions import col,sum,count,when,rank,window

# COMMAND ----------

df = spark.read.parquet("/mnt/sagen2databricks/silver/results")
df = df.select("race_id","driver_id","points","position","position_order")

# COMMAND ----------

df1 = spark.read.parquet("/mnt/sagen2databricks/silver/races")
df1 = df1.select("race_Id","race_year")

# COMMAND ----------

join_df = df.join(df1,"race_id","left")


# COMMAND ----------

driver_standing = join_df.groupBy("race_year", "driver_id") \
    .agg(sum(col("points")).alias("total_points"), 
         count(when(col("position") == 1, True)).alias("wins"))
driver_standing = driver_standing.withColumn("driverstanding_id", rank().over(Window.partitionBy("race_year").orderBy(col("total_points").desc())))
