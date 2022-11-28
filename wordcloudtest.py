from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv(r"Bakery_sales.csv", encoding ="latin-1")

comment_words = ''
stopwords = set(STOPWORDS)

