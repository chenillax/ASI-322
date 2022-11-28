from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import ChainMap
import pandas as pd
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DoubleType, FloatType,DateType
spark = SparkSession. builder.master("local").appName("Import File").getOrCreate()

data = "Bakery_sales.csv"

schema = StructType()\
    .add("",IntegerType(),True)\
    .add("Date", DateType(), True)\
    .add("Time",StringType(),True)\
    .add("TicketNumber",FloatType(),True)\
    .add("Article",StringType(),True)\
    .add("Quantity",FloatType(),True)\
    .add("UnitPrice",StringType(),True)\

df= spark.read\
    .option("header",True)\
    .schema(schema)\
    .csv(data)



article = df.groupBy("Article").count().sort("count")
article.write.csv('mycsv.csv')
"""
wordcloud = WordCloud(width = 800, height = 400, background_color="white")
words = dict(ChainMap(*article.select(F.create_map('Article', 'count')).rdd.map(lambda x: x[0]).collect()))

plt.imshow(wordcloud.generate_from_frequencies(words))

plt.show()"""