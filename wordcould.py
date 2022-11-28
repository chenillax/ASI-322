from collections import ChainMap

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from wordcloud import WordCloud

from schema import df_sales

# Compte les articles
article = df_sales.groupBy("Article").sum("Quantity").sort(F.col("sum(Quantity)").desc())
# article.write.csv('mycsv.csv')
size=800
x, y = np.ogrid[:size, :size]

mask = (x - (size/2)) ** 2 + (y - (size/2)) ** 2 > 400 ** 2
mask = 255 * mask.astype(int)

wordcloud = WordCloud(background_color="white", repeat=True,
                      mask=mask,max_words=200)
words = dict(ChainMap(*article.select(F.create_map('Article',
             'sum(Quantity)')).rdd.map(lambda x: x[0]).collect()))

plt.imshow(wordcloud.generate_from_frequencies(words))
plt.axis('off')
plt.show()
