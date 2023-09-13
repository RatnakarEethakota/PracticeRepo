# practice on pyspark dataframes


import pyspark
from util import *
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col,lit


spark = SparkSession.builder.getOrCreate()

data = [(101,'palakollu'),(105,'narsapur'),(104,'bhimavaram'),(107,'ravulapalem')]
columns = StructType([
    StructField("pincode",StringType(),True),
    StructField("villagename",StringType(),True)
])
df=create_dataframe(spark,data, columns)
df.show()

schema = StructType([
    StructField("user_id",IntegerType(),True),
    StructField("emailid",StringType(),True),
    StructField("nativelanguage",StringType(),True),
    StructField("location",StringType(),True)
])
# data1=spark.read.options(header=True).schema(x).csv("C:/Users/ASUS/Downloads/user.csv")
data1=[
    (101,'abc@gamil.com','telugu','Andhra'),
    (102,'uhg@gmail.com','kannada','Karnataka'),
    (103,'kjhhytf@gmail.com','tamil','TamilNadu'),
    (104,'ujhgtp@gmail.com','telugu','Telangana'),
    (105,'wsdftg@gmail.com','malayali','Kerala')
]
df1=create_dataframe(spark,data1,schema)
df1.show()

df2=select(spark,df1,data1,schema)
df2.show()