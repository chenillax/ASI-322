from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DoubleType, FloatType,DateType
spark = SparkSession. builder.master("local").appName("Import File").getOrCreate()

data = "Bakery_sales.csv"

schema = StructType()\
    .add("",IntegerType(),True)\
    .add("Date", StringType(), True)\
    .add("Time",StringType(),True)\
    .add("TicketNumber",FloatType(),True)\
    .add("Article",StringType(),True)\
    .add("Quantity",FloatType(),True)\
    .add("UnitPrice",StringType(),True)\

df_sales= spark.read\
    .option("header",True)\
    .schema(schema)\
    .csv(data)
