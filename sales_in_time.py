from schema import df_sales
import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DateType
from plotly.subplots import make_subplots


def mult(quantity, price):
    return round(quantity*float(price.replace(',', '.')), 2)


sales_quantity_by_type = df_sales.groupBy("Type").sum("Quantity").sort(F.col("sum(Quantity)").desc())  # .toPandas().plot.bar(x="Type", y="sum(Quantity)")
multiply = udf(mult, FloatType())
sales_subtotal_by_type = df_sales.withColumn("subtotal", multiply("Quantity", "UnitPrice")).groupBy("Type").sum(
    "subtotal").sort(F.col("sum(subtotal)").desc())  # .toPandas().plot.bar(x="Type", y="sum(subtotal)")
sales_quant_value = sales_quantity_by_type.join(sales_subtotal_by_type, sales_quantity_by_type.Type == sales_subtotal_by_type.Type, "inner").select(
    [sales_quantity_by_type.Type, "sum(Quantity)", "sum(subtotal)"]).toPandas()
sales_quant_value.plot.barh(x='Type',rot=0, title="Ventes: quantités par rapport au chiffre d'affaire par type")
plt.show()
