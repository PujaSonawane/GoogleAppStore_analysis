# Databricks notebook source
from pyspark.sql import SparkSession
sp=SparkSession.builder.appName('spark_project1').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

 #/FileStore/tables/googleplaystore.csv

# COMMAND ----------

apps=sp.read.load('/FileStore/tables/googleplaystore.csv',format='csv',sep=',',header='true',escape='"',inferSchema='true')

# COMMAND ----------

apps.show()

# COMMAND ----------

apps.count()

# COMMAND ----------

apps.printSchema()

# COMMAND ----------

#data= apps.drop("Size","Rating","Last Updated","Android Ver")
data=apps

# COMMAND ----------



# COMMAND ----------

data.printSchema()

# COMMAND ----------

data=data.withColumnRenamed('Last Updated','last_updated').withColumnRenamed("Current Ver",'curr_version').withColumnRenamed("Android Ver",'android_version').withColumnRenamed('Content Rating', 'content_rating')
data.printSchema()

# COMMAND ----------

data.select("Price").distinct().show()

# COMMAND ----------

data=data.withColumn("Reviews",col("Reviews").cast(IntegerType())).withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]","")).withColumn("Installs",col("installs").cast(IntegerType())).withColumn("Price",regexp_replace(col("Price"),"[$]","")).withColumn("Price",col("Price").cast(IntegerType()))


# COMMAND ----------

data.show(2)

# COMMAND ----------

data.createOrReplaceTempView("appsv")

# COMMAND ----------

# MAGIC %sql select * from appsv

# COMMAND ----------

# DBTITLE 1,Top reviews given to app
# MAGIC %sql select App, sum(Reviews) as total_reviews from appsv group by App order by total_reviews desc

# COMMAND ----------

# DBTITLE 1,Top installs
# MAGIC %sql select app, sum(Installs) as installs from appsv group by 1 order by 2 desc
# MAGIC limit 20

# COMMAND ----------

# DBTITLE 1,Category Wise Installs
# MAGIC %sql select category, sum(installs) total_installs from appsv group by 1 order by 2 desc limit 20

# COMMAND ----------



abc= sp.sql('select category, sum(installs) total_installs from appsv group by 1 order by 2 desc limit 20')


# COMMAND ----------

abc.createOrReplaceTempView("top_installs")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql select distinct Type from appsv

# COMMAND ----------

# MAGIC %sql select app,price from appsv where Type='Paid' limit 100

# COMMAND ----------

# DBTITLE 1,Top Paid apps
# MAGIC %sql select app, sum(price) as price from appsv
# MAGIC where Type='Paid'
# MAGIC group by 1
# MAGIC order by 2 desc limit 10

# COMMAND ----------

# MAGIC %sql select * from appsv where Type='Paid' limit 100

# COMMAND ----------

data.columns

# COMMAND ----------

# DBTITLE 1,Game category analysis
# MAGIC %sql select category, Genres, sum(installs) installs, sum(price) as price from appsv  
# MAGIC where lower(Category)="game"
# MAGIC group by 1,2
# MAGIC
# MAGIC --arcade made more installs but action type made more money

# COMMAND ----------

# MAGIC %sql select category, Genres, content_rating ,sum(installs) installs, sum(price) as price from appsv  
# MAGIC where lower(Category)="game" and lower(genres)='action'
# MAGIC group by 1,2,3
# MAGIC --as expected teen spent more money on action games

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,category vs content_rating
# MAGIC %sql select category, content_rating, sum(installs) as installs, sum(price) as price from appsv
# MAGIC where category in (select distinct category from top_installs)
# MAGIC group by 1,2
# MAGIC
# MAGIC

# COMMAND ----------


