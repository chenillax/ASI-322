from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import ChainMap
import pandas as pd
import pyspark.sql.functions as F

from schema import df

#Compte les articles 
article = df.groupBy("Article").count().sort("count")

#article.write.csv('mycsv.csv')

wordcloud = WordCloud(width = 800, height = 400, background_color="white")
words = dict(ChainMap(*article.select(F.create_map('Article', 'count')).rdd.map(lambda x: x[0]).collect()))

plt.imshow(wordcloud.generate_from_frequencies(words))

plt.show()