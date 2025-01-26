# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#Create a external schema using StructType and StructField functions

mySchema=StructType([
    StructField("emp_id",IntegerType(),True),
    StructField("emp_name",StringType(),True),
    StructField("salary",IntegerType(),True),
    StructField("manager_id",IntegerType(),True),
    StructField("dept_name",StringType(),True),
])

myData=[
(1 ,'Sudhir',45000, 18,"HR"),
(2 ,'Dany',55000,  16,"Account"),
(3 ,'Sushya',40000,  18,"HR"),
(4 ,'Chandrya',66000,  17,"Account"),
(5,'Jaysya',88000,  18,"HR"),
(6,'Rohit',49000,  18,"IT"),
(7,'Ramesh',99000, 10,"Account"),
(8,'Sachin',80000, 16,"Sales"),
(9,'Ajit',66000,   17,"IT"),
(10 ,'Anil',54000, 18,"Sales"),
(11 ,'Vikas',75000,  16,"Marketing"),
(12 ,'Nisha',40000,  18,"IT"),
(13 ,'Nidhi',60000,  17,"Sales"),
(14 ,'Priya',80000,  18,"Marketing"),
(15 ,'Mohit',45000,  18,"IT"),
(16 ,'Rajesh',90000, 10,"Marketing"),
(17 ,'Raman',55000, 16,"Sales"),
(18 ,'Sam',65000,   17,"IT"),
(19,'Sainath',70000,21,"IT"),
(20,'Rahula',80000,22,"Sales")    
]

# Crate a dataframe using schema and data

df=spark.createDataFrame(data=myData,schema=mySchema)
df.show(5)
df.printSchema()

# count the number of records
print('No.of records',df.count())

# COMMAND ----------

df1=df.groupBy("dept_name").count().show()
df2=df.groupBy("dept_name").max("salary").show()
df3=df.groupBy("dept_name").sum("salary").show()

# COMMAND ----------

# groupBy agg() function is used  to calculate more than one aggregate(multiple aggregate) at a time on grouped dataframe
df.show()
df_agg=df.groupBy("dept_name").count().show()
df.agg=df.groupBy("dept_name").agg(min("salary").alias("minSalary"),
                                   count("emp_name").alias("countOfEmp")).show()

df.createOrReplaceTempView("dfTable")
spark.sql('''SELECT dept_name,
          COUNT(emp_name),
          MIN(salary) 
          FROM dfTable
          GROUP BY dept_name
           ''').show()

