from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DoubleType, FloatType,DateType
spark = SparkSession. builder.master("local").appName("Import File").getOrCreate()

data = "article_type.csv"

schema = StructType()\
    .add("Article",StringType(),True)\
    .add("Quantity",FloatType(),True)\
    .add("Type",StringType(),True)\

df_type= spark.read\
    .option("header",False)\
    .schema(schema)\
    .csv(data)
