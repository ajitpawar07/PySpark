# Databricks notebook source
# MAGIC %md
# MAGIC ###DataReading

# COMMAND ----------

df_csv=spark.read.format("csv")\
                 .option("inferSchema",True)\
                 .option("header",True)\
                 .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

df_csv.show()

# COMMAND ----------

df_json=spark.read.format('json')\
                  .option('inferSchema',True)\
                  .option('header',True)\
                  .option('multiLine',False)\
                  .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_csv.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema

# COMMAND ----------

my_ddl_schema='''
                 Item_Identifier STRING,
                 Item_Weight STRING,
                 Item_Fat_Content STRING,
                 Item_Visibility DOUBLE,
                 Item_Type STRING,
                 Item_MRP DOUBLE,
                 Outlet_Identifier STRING,
                 Outlet_Establishment_Year INT,
                 Outlet_Size STRING,
                 Outlet_Location_Type STRING,
                 Outlet_Type STRING,
                 Item_Outlet_Sales DOUBLE
             '''   


# COMMAND ----------

df_csv.printSchema()

# COMMAND ----------

df_csv=spark.read.format('csv')\
                 .schema(my_ddl_schema)\
                 .option('header',True)\
                 .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ###StructType Schema

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

my_structType_schema=StructType([
    StructField("Item_Identifier", StringType(), nullable=True),
    StructField("Item_Weight", StringType(), nullable=True),
    StructField("Item_Fat_Content", StringType(), nullable=True),
    StructField("Item_Visibility", DoubleType(), nullable=True),
    StructField("Item_Type", StringType(), nullable=True),
    StructField("Item_MRP", DoubleType(), nullable=True),
    StructField("Outlet_Identifier", StringType(), nullable=True),
    StructField("Outlet_Establishment_Year", IntegerType(), nullable=True),
    StructField("Outlet_Size", StringType(), nullable=True),
    StructField("Outlet_Location_Type", StringType(), nullable=True),
    StructField("Outlet_Type", StringType(), nullable=True),
    StructField("Item_Outlet_Sales", DoubleType(), nullable=True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select expression

# COMMAND ----------

#Without using col object
df_select=df_csv.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

#Using col object
df_col=df_csv.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Alias

# COMMAND ----------

df_alias=df_csv.select(col('Item_Identifier').alias('Item_Id')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filter/where

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario 1

# COMMAND ----------

df_filter1=df_csv.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario 2
# MAGIC

# COMMAND ----------

df_filter2=df_csv.filter((col('Item_Type')=='Soft Drinks')&(col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario 3

# COMMAND ----------

#df_filter3=df_csv.filter((col('Outlet_Location_Type')=='Tier 1')|(col('Outlet_Location_Type')=='Tier 2')&(col('Outlet_Size')=='null')).display()

df_csv.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

df_csv.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2')))
