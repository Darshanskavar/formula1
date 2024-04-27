# Databricks notebook source
# MAGIC %run /Workspace/formula_one_through_ADF/bronze/cmn_fun

# COMMAND ----------

from pyspark.sql.functions import col,explode


# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls('/mnt/adlsformula/f1racing')

# COMMAND ----------

df = spark.read.option("multiline", "true").json(f"/mnt/adlsformula/f1racing/bronze/seasons",recursiveFileLookup=True)

# COMMAND ----------

df = df.withColumn('list',explode(col('MRData').SeasonTable.Seasons))

# COMMAND ----------

df = df.drop('MRData')


# COMMAND ----------

df = df.withColumn('season',col('list').season).withColumn('url',col('list').url)

df = df.drop('list')
df.display()

# COMMAND ----------

df = df.select('season','url').distinct()
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.option('format','delta').mode('overwrite').save('/mnt/adlsformula/f1racing/silver/seasons')
