"""
query and viz file
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# sample query
def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = ("""
    SELECT country, beer_servings
    FROM alcohol 
    WHERE country in ("China","Egypt","Germany","Japan","USA")
""")
    query_result = spark.sql(query)
    return query_result


# sample viz for project
def viz():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")

    pandas_df = query.select("country", "beer_servings").toPandas()

    # Plot a bar plot
    plt.figure(figsize=(15, 8))
    plt.bar(pandas_df["country"], pandas_df["beer_servings"], color='skyblue')
    plt.title("beer consumption")
    plt.xlabel("country")
    plt.ylabel("beer_servings")
    plt.show()
    

if __name__ == "__main__":
    query_transform()
    viz()