# Databricks notebook source
# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

driver_df = spark.read.parquet("/mnt/sagen2databricks/silver/drivers")
constructor_df = spark.read.parquet("/mnt/sagen2databricks/silver/constructors")
result_df = spark.read.parquet("/mnt/sagen2databricks/silver/results")
pit_stops = spark.read.parquet("/mnt/sagen2databricks/silver/pit_stops")
races_df = spark.read.parquet("/mnt/sagen2databricks/silver/races")

# COMMAND ----------

drivers_df1 = driver_df.withColumn("driver",concat("forename",lit(" "),"surname"))\
                    .withColumn("number",when(driver_df["number"].isNull()," ").otherwise(driver_df["number"]))\
                    .select("driver_Id","number","driver",col("driver_nationality").alias("nationality"))
constructor_df1 = constructor_df.select(col("constructor_name").alias("team"),"constructor_id")
result_df1 = result_df.select(col("fastest_lap_time").alias("Fastest_lap"),"points","grid","constructor_id","driver_id","race_id")
pit_stops1 = pit_stops.select("driver_id",col("stop").alias("pits"),"race_id")
races_df1 = races_df.select(col("time").alias("race_time"),"race_Id")

# COMMAND ----------

race_result = drivers_df1.join(result_df1,"driver_id","left").join(races_df1,"race_id",'left').join(pit_stops1,'driver_id','left').join(constructor_df1,"constructor_id","left")

# COMMAND ----------

race_result_df = race_result.select("nationality","driver","number","team","grid","pits","Fastest_lap","race_time","points").display()

# COMMAND ----------

final_df = race_result_df.write.mode("overwrite").parquet("/mnt/sagen2databricks/gold/race_result")
