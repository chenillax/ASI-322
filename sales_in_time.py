from schema import df_sales
import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import when, col, udf


def mult(quantity, price):
    return round(quantity*float(price.replace(',', '.')), 2)


sales_quantity_by_type = df_sales.groupBy("Type").sum("Quantity").sort(F.col("sum(Quantity)").desc()).toPandas().plot.bar(x="Type", y="sum(Quantity)")
multiply = udf(mult)
df_sales.withColumn("subtotal", multiply("Quantity", "UnitPrice")).show()
plt.show()
