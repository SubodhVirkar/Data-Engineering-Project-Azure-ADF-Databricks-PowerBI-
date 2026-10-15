# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###  silver layer script

# COMMAND ----------

# MAGIC %md
# MAGIC Data access using application

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.subodhstoragedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.subodhstoragedatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.subodhstoragedatalake.dfs.core.windows.net", "0e92dada-01c0-4503-a01c-1d019fc60cf3")
spark.conf.set("fs.azure.account.oauth2.client.secret.subodhstoragedatalake.dfs.core.windows.net", "Yi38Q~rz2IkYwDbSFz_-2dw_bvOyJ1Wh5MDrocgB")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.subodhstoragedatalake.dfs.core.windows.net", "https://login.microsoftonline.com/d8457ff4-6df9-45bf-999b-ad513673f31a/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC reading  data

# COMMAND ----------

df_calender = spark.read.format('csv').option("header", True).option("inferSchema", True).load("abfss://bronze@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cust = spark.read.format('csv').option("header", True).option("inferSchema", True).load("abfss://bronze@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_prod_cat = spark.read.format('csv').option("header", True).option("inferSchema", True).load("abfss://bronze@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_prod = spark.read.format('csv').option("header", True).option("inferSchema", True).load("abfss://bronze@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_ret = spark.read.format('csv').option("header", True).option("inferSchema", True).load("abfss://bronze@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_sales = spark.read.format('csv').option("header", True).option("inferSchema", True).load("abfss://bronze@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_ter = spark.read.format('csv').option("header", True).option("inferSchema", True).load("abfss://bronze@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_sub_cat = spark.read.format('csv').option("header", True).option("inferSchema", True).load("abfss://bronze@subodhstoragedatalake.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation to silver layer

# COMMAND ----------

# MAGIC %md
# MAGIC calender transformation

# COMMAND ----------

df_calender.display()

# COMMAND ----------

df_calender.withColumn('Month',month(col('Date')))\
            .withColumn('Year',year(col('Date')))\
            .display()

# COMMAND ----------

df_calender.write.format('parquet')\
    .mode('append')\
    .option("path", "abfss://silver@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar")\
        .save()


# COMMAND ----------

# MAGIC %md
# MAGIC customer transformation

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust = df_cust.withColumn('Full Name', concat(col('Prefix'), lit(' '), col('FirstName'), lit(' '), col('LastName'))).display()

# COMMAND ----------

df_cust = df_cust.withColumn('Fullname',concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust.write.format('parquet')\
    .mode('append')\
    .option("path", "abfss://silver@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Customer")\
        .save() 

# COMMAND ----------

# MAGIC %md
# MAGIC subcategories transformation

# COMMAND ----------

df_sub_cat.display()


# COMMAND ----------

df_sub_cat.write.format('parquet')\
    .mode('append')\
    .option("path", "abfss://silver@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_SubCategory")\
        .save() 


# COMMAND ----------

# MAGIC %md 
# MAGIC products data transformation
# MAGIC

# COMMAND ----------

df_prod.display() 

# COMMAND ----------

df_prod = df_prod.withColumn('ProductSKU',split(col('ProductSKU'), '-')[0]).withColumn('ProductName',split(col('ProductName'), ' ')[0])

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.write.format('parquet')\
    .mode('append')\
    .option("path", "abfss://silver@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Product")\
        .save()

# COMMAND ----------

df_ret.display() 

# COMMAND ----------

df_ret.write.format('parquet')\
    .mode('append')\
    .option("path", "abfss://silver@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Return")\
        .save()  

# COMMAND ----------

# MAGIC %md
# MAGIC terriotories    

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
    .mode('append')\
    .option("path", "abfss://silver@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Territory")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp(col('StockDate')))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply', col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('append')\
    .option("path", "abfss://silver@subodhstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales")\
        .save() 

# COMMAND ----------

# MAGIC %md
# MAGIC Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('Total_Order')).display()

# COMMAND ----------

df_sub_cat.display() 

# COMMAND ----------

df_ter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your DataFrame to the Catalog

# COMMAND ----------

df_sales.write.format("delta").mode("overwrite").saveAsTable("silver_table_sales")

# COMMAND ----------



# COMMAND ----------

