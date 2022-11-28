from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import ChainMap
import pandas as pd
import pyspark.sql.functions as F
import numpy as np

from schema import df

# Compte les articles
article = df.groupBy("Article").count().sort("count")

# article.write.csv('mycsv.csv')
size=800
x, y = np.ogrid[:size, :size]

mask = (x - (size/2)) ** 2 + (y - (size/2)) ** 2 > 400 ** 2
mask = 255 * mask.astype(int)

wordcloud = WordCloud(background_color="white", repeat=True,
                      mask=mask,max_words=200)
words = dict(ChainMap(*article.select(F.create_map('Article',
             'count')).rdd.map(lambda x: x[0]).collect()))

plt.imshow(wordcloud.generate_from_frequencies(words))
plt.axis('off')
plt.show()
