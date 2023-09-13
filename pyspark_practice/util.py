# practice on pyspark dataframes


import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col,lit


spark = SparkSession.builder.getOrCreate()
def create_dataframe(spark,data, columns):
    df = spark.createDataFrame(data=data, schema=columns)
    return df
def create_dataframe(spark,data1,schema):
    df1=spark.createDataFrame(data=data1,schema=schema)
    return df1
def select(spark,df1,data1,schema):
    df2= df1.select(col("emailid"),col("location"))
    return df2

# #select()
# df1.select(col("emailid"),col("location")).show()
# #coolect()
# datacollect=df.collect()
# print(datacollect)
# #withcolumn()
# df2=df1.withColumn("Country", lit("USA")) \
#    .withColumn("anotherColumn",lit("anotherValue"))
# df2.show()
# #withcolumnrenamed()
# df.withColumnRenamed("pincode","postalcode").show()