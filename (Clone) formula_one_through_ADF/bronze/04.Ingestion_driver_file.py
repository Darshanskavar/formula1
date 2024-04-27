# Databricks notebook source
# MAGIC %run /Workspace/formula_one_through_ADF/bronze/cmn_fun

# COMMAND ----------

from pyspark.sql.functions import explode,col

# COMMAND ----------

df = spark.read.option('multiline',True).json('/mnt/adlsformula/f1racing/bronze/drivers/',recursiveFileLookup=True)
df.display()



# COMMAND ----------

df = df.withColumn('list',explode(col('MRData').DriverTable.Drivers))
df = df.drop('MRData')
df.display()


# COMMAND ----------

df = df.withColumn('Driver_id',col('list').driverid)\
        .withColumn('DOB',col('list').dateOfBirth)\
        .withColumn('Driver_nationality',col('list').nationality)\
        .withColumn('surname',col('list').familyName)\
        .withColumn('forename',col('list').givenName)\
        .withColumn('driver_ref',col('list').familyName)\
        .withColumn('code',col('list').code)\
        .withColumn('number',col('list').permanentNumber)
df = df.drop('list')
df.display()
print(df.count())

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.select('Driver_id','DOB','Driver_nationality','surname','forename','driver_ref','code','number').distinct()
df.count()

# COMMAND ----------

df.write.option('format','delta').mode('overwrite').save('/mnt/adlsformula/f1racing/silver/drivers')
