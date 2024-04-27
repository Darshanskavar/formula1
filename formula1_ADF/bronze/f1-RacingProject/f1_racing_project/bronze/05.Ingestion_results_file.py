# Databricks notebook source
file_name = dbutils.widgets.text("file_name","")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

df = spark.read.json(f"/mnt/sagen2databricks/bronze/{file_name}.json",schema=results_schema)
display(df)

# COMMAND ----------

rename_df = df.withColumnRenamed("resultId", "result_id") \
            .withColumnRenamed("raceId", "race_id") \
            .withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("constructorId", "constructor_id") \
            .withColumnRenamed("positionText", "position_text") \
            .withColumnRenamed("positionOrder", "position_order") \
            .withColumnRenamed("fastestLap", "fastest_lap") \
            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
            .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") 

# COMMAND ----------

output_df = add_column(rename_df)
display(output_df)

# COMMAND ----------

output_df.write.mode("overwrite").parquet(f"/mnt/sagen2databricks/silver/{file_name}")
