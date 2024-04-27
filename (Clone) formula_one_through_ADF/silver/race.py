# Databricks notebook source
from pyspark.sql.functions import col,concat_ws
drivers = spark.read.load('/mnt/adlsformula/f1racing/silver/drivers')
drivers = drivers.withColumn('full name',concat_ws(' ',col('forename'),col('surname'))).select('driver_id','full name','number')

# COMMAND ----------

from pyspark.sql.functions import col,concat_ws
constructors = spark.read.load('/mnt/adlsformula/f1racing/silver/constructors')
constructors = constructors.select('constructor_id',col('name').alias('constructor_name'))

# COMMAND ----------

results = spark.read.load('/mnt/adlsformula/f1racing/silver/results')
results = results.select('race_id','driver_id','constructor_id','grid','points')

# COMMAND ----------

races = spark.read.load('/mnt/adlsformula/f1racing/silver/race')
races = races.select('race_id','season','date')

# COMMAND ----------

pit_stops = spark.read.load('/mnt/adlsformula/f1racing/silver/pit_stops')
pit_stops.display()
# pit_stops = pit_stops.select('race_id','driver_id','stop')


# COMMAND ----------

from pyspark.sql.functions import col,desc
df = drivers.join(results,'driver_id','left')
df = df.join(constructors,'constructor_id','left')
df = df.join(races,'race_id','left')
df = df.join(pit_stops,['race_id','driver_id'],'left')
df = df.select(col('full name').alias('Driver'),
               col('number'),
               col('constructor_name').alias('Team'),
               col('grid'),
               col('pit').alias('Pits'),
               col('points'),
               col('season').alias('Year'),
               col('date')
               ).orderBy(desc('points')).filter((col('Pits')=='2') & (col('date')=='2024-03-24'))
df = df.display() 

# COMMAND ----------

from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window
df.withColumn('rnk',dense_rank().over(Window.partitionBy('Driver','Year','Grid').orderBy('Pits'))).display()                  

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Load a DataFrame
df = spark.read.csv("path_to_csv_file.csv", header=True, inferSchema=True)

# Apply the dense_rank function and display the results
df.withColumn('rnk',dense_rank().over(Window.partitionBy('Driver','Year','Grid').orderBy('Pits'))).display()
