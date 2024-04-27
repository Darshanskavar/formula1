# Databricks notebook source
file_name = dbutils.widgets.text("file_name","")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

input_schema = StructType([
    StructField("constructorId",IntegerType()),
    StructField("constructorRef",StringType()),
    StructField("name",StringType()),
    StructField("nationality",StringType()),
    StructField("url",StringType())
])

# COMMAND ----------

constructor_df = spark.read.json(f"/mnt/sagen2databricks/bronze/{file_name}.json",schema=input_schema).display()


# COMMAND ----------

output_df = add_column(constructor_df)

# COMMAND ----------

rename_df = output_df.withColumnRenamed("constructorId","constructor_Id")\
                   .withColumnRenamed("constructorRef","constructor_Ref")\
                   .withColumnRenamed("url","constructor_url")\
                    .withColumnRenamed("name","constructor_name")\
                    .withColumnRenamed("nationality","constructor_nationality")

# COMMAND ----------

rename_df.write.mode("overwrite").parquet(f"/mnt/sagen2databricks/silver/{file_name}")
