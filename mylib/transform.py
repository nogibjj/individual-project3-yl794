"""
transform and load function
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/individual3/alcohol_consumption.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    daily_show_guest_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    daily_show_guest_df = daily_show_guest_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it 
    daily_show_guest_df.write.format("delta").mode("overwrite").saveAsTable("alcohol")

    num_rows = daily_show_guest_df.count()
    print(num_rows)
    
    return "finished transform and load"

if __name__ == "__main__":
    load()