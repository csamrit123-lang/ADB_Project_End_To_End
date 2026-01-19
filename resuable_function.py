# resuable_function.py
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank,dense_rank,row_number

class window_function:
    def rank(self, df):
        w_n = Window.partitionBy('Year').orderBy(col('total_amount').desc())
        df = df.withColumn('rnk', rank().over(w_n))
        return df
    def dense_rank(self, df):
        w_n = Window.partitionBy('Year').orderBy(col('total_amount').desc())
        df = df.withColumn('dense_rnk', dense_rank().over(w_n))
        return df
    def row_num(self, df):
        w_n = Window.partitionBy('Year').orderBy(col('total_amount').desc())
        df = df.withColumn('row_number', row_number().over(w_n))
        return df
