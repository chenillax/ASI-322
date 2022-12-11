from schema import df_sales
import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DateType
from pyspark.sql.window import Window
import seaborn as sns


def mult(quantity, price):
    return round(quantity*float(price.replace(',', '.')), 2)


multiply = udf(mult, FloatType())

sales_quantity_by_type = df_sales.groupBy("Type").sum("Quantity").sort(F.col("sum(Quantity)").desc())  # .toPandas().plot.bar(x="Type", y="sum(Quantity)")
sales_subtotal_by_type = df_sales.withColumn("subtotal", multiply("Quantity", "UnitPrice")).groupBy("Type").sum(
    "subtotal").sort(F.col("sum(subtotal)").desc())  # .toPandas().plot.bar(x="Type", y="sum(subtotal)")
sales_quant_value = sales_quantity_by_type.join(sales_subtotal_by_type, sales_quantity_by_type.Type == sales_subtotal_by_type.Type, "inner").select(
    [sales_quantity_by_type.Type, "sum(Quantity)", "sum(subtotal)"]).toPandas()
sales_quant_value.plot.bar(x='Type', rot=0, title="Comparaison CA/quantité")
# plt.show()


sales_by_day = df_sales.withColumn("subtotal", multiply("Quantity", "UnitPrice")).groupBy("Date").sum(
    "subtotal").select(to_date(col("Date"), "dd/MM/yyyy").alias("Date"), col("sum(subtotal)").alias("CA")).sort("Date").toPandas()
sales_by_day['CA_14day_ave'] = sales_by_day.CA.rolling(14).mean().shift(-6)

sns.set_context("talk")
plt.figure(figsize=(9, 6))
sns.lineplot(x='Date', y='CA', data=sales_by_day, label='Chiffre d\'affaire')
sns.lineplot(x='Date', y='CA_14day_ave', data=sales_by_day, palette='red', label="Moyenne flottante sur 2 semaines")
pos = ['2021-01-01',  '2021-04-01',
       '2021-07-01',  '2021-10-01',
       '2022-01-01',  '2022-04-01',
       '2022-07-01',  '2022-10-01'
       ]

lab = ['Jan 21', 'Avr 21', 'Juil 21',
       'Oct 21', 'Jan 22', 'Avr 22',
       'Juil 22', 'Oct 22']

plt.xticks(pos, lab)
plt.title("Suivi du chiffre d'affaire sur l'ensemble des données")
plt.xlabel("Date", size=14)
plt.ylabel("CA", size=14)
plt.show()
