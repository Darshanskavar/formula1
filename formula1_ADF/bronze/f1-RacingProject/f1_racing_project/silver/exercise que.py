# Databricks notebook source
# MAGIC %md
# MAGIC #####Que 1. get a driver name who has max points per year

# COMMAND ----------

# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

# MAGIC %run ../silver/race_result

# COMMAND ----------

races_df1 = races_df.select("race_year",col("time").alias("race_time"))

# COMMAND ----------

race_result = drivers_df1.join(result_df1,"driver_id","left").join(races_df,"race_id").join(pit_stops1,"race_id").join(constructor_df1,"constructor_id","left")

# COMMAND ----------

max_points_per_year_df = race_result.select("name","race_year","points").groupBy("race_year").agg(max("points").alias("max_points")).display()


# COMMAND ----------

w = Window.partitionBy("race_year").orderBy(col("points").desc())

max_points_per_year_df = race_result.select("driver", "race_year", "points", max("points").over(w).alias("max_points")) \
                                    .where(col("points") == col("max_points")) \
                                    .distinct()

display(max_points_per_year_df)

