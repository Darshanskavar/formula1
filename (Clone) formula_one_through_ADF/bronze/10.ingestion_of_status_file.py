# Databricks notebook source
# MAGIC %run /Workspace/formula_one_through_ADF/bronze/cmn_fun

# COMMAND ----------

from pyspark.sql.functions import col,explode

# COMMAND ----------

status_df = spark.read.json('/mnt/adlsformula/f1racing/bronze/status',recursiveFileLookup=True)
status_df.display()

# COMMAND ----------

status_df = status_df.withColumn('list',explode(col('MRData').StatusTable.Status))
# status_df = status_df.withColumn('lst',col('list'))
status_df.display()


# COMMAND ----------

from pyspark.sql.functions import col

status_df = status_df.withColumn('staus_id', col('list').statusId) \
                     .withColumn('status', col('list').status)
status_df = status_df.drop('MRData', 'list')
status_df.display()

# COMMAND ----------

status_df.write.option('format','delta').mode('overwrite').save('/mnt/adlsformula/f1racing/silver/status')
