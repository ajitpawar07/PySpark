# Databricks notebook source
#import necessary spark libraries
from pyspark.sql import SparkSession

#create sparksession
spark=SparkSession.builder.appName("windowFunction").getOrCreate()

#create dataframe
window_schema=["Name","Dept","Salary"]

window_data=[("Vishal","HR",2000),
             ("Akash","IT",3000),
             ("Divya","HR",1500),
             ("Ajit","Payroll",300),
             ("Shubham","IT",3000),
             ("Ram","IT",4000),
             ("Krishna","Payroll",2000),
             ("Hari","IT",2000),
             ("Jack","HR",2000),
             ("Trump","IT",2500)
            ]

window_df=spark.createDataFrame(window_data,window_schema)   
window_df.show() 
window_df.sort("Dept").show() 

#Creating temporary table for sql queries
window_df.createOrReplaceTempView("window_table")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

#ROW NUMBER
#In pyspark
windowSpec=Window.partitionBy("Dept").orderBy(desc("Salary"))

row_number_df=window_df.withColumn("RowNumber",row_number().over(windowSpec))
row_number_df.show()

#In SQL
window_sql=spark.sql('''
                     SELECT *,
                     ROW_NUMBER() OVER (PARTITION BY Dept ORDER BY salary DESC) AS Row_Number_sql
                     FROM window_table
                     ''')
window_sql.show()                     

# COMMAND ----------

#RANK
#In pyspark

rank_df=window_df.withColumn("Rank",rank().over(windowSpec))
rank_df.show()

#In SQL
rank_df_sql=spark.sql('''
                      SELECT *,
                      RANK() OVER(PARTITION BY Dept ORDER BY salary DESC) AS Rank_sql
                      FROM window_table
                      ''')
rank_df_sql.show()                      

# COMMAND ----------

#DENSE RANK
#In pyspark
dense_rank_df=window_df.withColumn("Dense_rank",dense_rank().over(windowSpec))
dense_rank_df.show()

#In SQL
dense_rank_sql=spark.sql('''
                         SELECT *,
                         DENSE_RANK() OVER (PARTITION BY Dept ORDER BY salary DESC) AS DenseRank
                         FROM window_table
                         ''')
dense_rank_sql.show()                         

# COMMAND ----------

# LEAD AND LAG
#In pyspark

lead_df=window_df.withColumn("next_data",lead("salary",1).over(windowSpec))
lead_df.show()

#In SQL

lead_df_sql=spark.sql('''
                      SELECT *,
                      LEAD(salary,1) OVER (PARTITION BY Dept ORDER BY salary DESC) AS Next_sal
                      FROM window_table
                      ''')

lead_df_sql.show() 

#LAG
#In pyspark
lag_df=window_df.withColumn("prev_sal",lag("salary",1).over(windowSpec))
lag_df.show()

#In SQL
lag_df_sql=spark.sql('''
                     SELECT *,
                     LAG(salary,1) OVER (PARTITION BY dept ORDER BY salary DESC) AS Prev_sal
                     FROM window_table
                    ''')

lag_df_sql.show()                    

