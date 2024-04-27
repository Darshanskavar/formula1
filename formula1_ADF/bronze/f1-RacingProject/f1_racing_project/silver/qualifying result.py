# Databricks notebook source
# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

drivers_df = spark.read.parquet("/mnt/sagen2databricks/silver/drivers")
constructors_df = spark.read.parquet("/mnt/sagen2databricks/silver/constructors")
qualifying_df = spark.read.parquet("/mnt/sagen2databricks/silver/qualifying")

# COMMAND ----------

qualifying_df1 =qualifying_df.select("driver_id","constructor_id","qualifying1","qualifying2","qualifying3")
drivers_df1 = drivers_df.withColumn("driver",concat("forename",lit(" "),"surname")).select("driver_Id","number","driver")
constructors_df1 = constructors_df.select("constructor_Id",col("constructor_name").alias("team"))

# COMMAND ----------

result_df = qualifying_df1.join(drivers_df1,"driver_id","left").join(constructors_df1,"constructor_id","left")

# COMMAND ----------

result_col_lst = ["driver","number","team","qualifying1","qualifying2","qualifying3"]
result_df1 = result_df.select(*result_col_lst).display()

# COMMAND ----------

result_df1 = result_df1.write.mode("overwrite").parquet("/mnt/sagen2databricks/gold/qualifying_result")
