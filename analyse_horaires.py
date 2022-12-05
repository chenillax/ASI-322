from schema import df_sales
from pyspark.sql.functions import when,col,udf
import pandas as pd
from datetime import datetime
from pyspark.sql.types import StringType


horaires_vente = df_sales.select(["Date","Time","TicketNumber"])\
                .dropDuplicates(["TicketNumber"])\
                .withColumn("TimeInterval",\
                    when(df_sales.Time.rlike("07:[0-2][0-9]$"),"7:00 - 7:30")\
                    .when(df_sales.Time.rlike("07:[3-5][0-9]$"),"7:30 - 8:00")\
                    .when(df_sales.Time.rlike("08:[0-2][0-9]$"),"8:00 - 8:30")\
                    .when(df_sales.Time.rlike("08:[3-5][0-9]$"),"8:30 - 9:00")\
                    .when(df_sales.Time.rlike("09:[0-2][0-9]$"),"9:00 - 9:30")\
                    .when(df_sales.Time.rlike("09:[3-5][0-9]$"),"9:30 - 10:00")\
                    .when(df_sales.Time.rlike("10:[0-2][0-9]$"),"10:00 - 10:30")\
                    .when(df_sales.Time.rlike("10:[3-5][0-9]$"),"10:30 - 11:00")\
                    .when(df_sales.Time.rlike("11:[0-2][0-9]$"),"11:00 - 11:30")\
                    .when(df_sales.Time.rlike("11:[3-5][0-9]$"),"11:30 - 12:00")\
                    .when(df_sales.Time.rlike("12:[0-2][0-9]$"),"12:00 - 12:30")\
                    .when(df_sales.Time.rlike("12:[3-5][0-9]$"),"12:30 - 13:00")\
                    .when(df_sales.Time.rlike("13:[0-2][0-9]$"),"13:00 - 13:30")\
                    .when(df_sales.Time.rlike("13:[3-5][0-9]$"),"13:30 - 14:00")\
                    .when(df_sales.Time.rlike("14:[0-2][0-9]$"),"14:00 - 14:30")\
                    .when(df_sales.Time.rlike("14:[3-5][0-9]$"),"14:30 - 15:00")\
                    .when(df_sales.Time.rlike("15:[0-2][0-9]$"),"15:00 - 15:30")\
                    .when(df_sales.Time.rlike("15:[3-5][0-9]$"),"15:30 - 16:00")\
                    .when(df_sales.Time.rlike("16:[0-2][0-9]$"),"16:00 - 16:30")\
                    .when(df_sales.Time.rlike("16:[3-5][0-9]$"),"16:30 - 17:00")\
                    .when(df_sales.Time.rlike("17:[0-2][0-9]$"),"17:00 - 17:30")\
                    .when(df_sales.Time.rlike("17:[3-5][0-9]$"),"17:30 - 18:00")\
                    .when(df_sales.Time.rlike("18:[0-2][0-9]$"),"18:00 - 18:30")\
                    .when(df_sales.Time.rlike("18:[3-5][0-9]$"),"18:30 - 19:00")\
                    .when(df_sales.Time.rlike("19:[0-2][0-9]$"),"19:00 - 19:30")\
                    .when(df_sales.Time.rlike("19:[3-5][0-9]$"),"19:30 - 20:00")\
                    .when(df_sales.Time.rlike("20:[0-2][0-9]$"),"20:00 - 20:30"))

def date_to_day(s):
    return datetime.strptime(s,"%Y-%m-%d").strftime('%A')

reg_sal = udf(lambda q : date_to_day(q), StringType())     
horaires_jour_vente = horaires_vente.withColumn("Day",reg_sal(col("Date")))
horaires_semaine = horaires_jour_vente.filter(horaires_jour_vente["Day"]!="Saturday")\
                                    .filter(horaires_jour_vente["Day"]!="Sunday")
horaires_weekend = horaires_jour_vente.filter(horaires_jour_vente["Day"]=="Saturday" | horaires_jour_vente["Day"]!="Sunday")                


#horaires_vente.write.csv('horaire.csv')
horaires_weekend.write.csv('horaire_jour.csv')