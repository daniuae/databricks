# Problem 1: Load CSV and explore

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CertificationPrep").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("/databricks-datasets/samples/population-vs-price/data_geo.csv")

df.printSchema()
df.show(5, truncate=False)

# Problem 2: Select & filter
# Find all counties with population > 200,000.

from pyspark.sql.functions import col

df.filter(col("2014 Population estimate") > 200000).select("State", "County").show(10, False)

# Module 2: Transformations & Actions
# Problem 3: Aggregation
# Find average MedianHomeValue per state.
from pyspark.sql.functions import avg

df.groupBy("State").agg(avg("MedianHomeValue").alias("Avg_Home_Value")).orderBy(col("Avg_Home_Value").desc()).show(10, False)


# Problem 4: Deduplication
# Remove duplicate (State, County) combinations.

df_unique = df.dropDuplicates(["State", "County"])
print("Row count after dedup:", df_unique.count())

# Module 3: Joins
# Problem 5: Join two datasets
# Datasets:
# /databricks-datasets/samples/population-vs-price/data_geo.csv
# /databricks-datasets/samples/population-vs-price/StateDemographics.csv
# 👉 Task: Join on State and get population + median age.

df1 = spark.read.option("header", True).option("inferSchema", True).csv("/databricks-datasets/samples/population-vs-price/data_geo.csv")
df2 = spark.read.option("header", True).option("inferSchema", True).csv("/databricks-datasets/samples/population-vs-price/StateDemographics.csv")

joined = df1.join(df2, on="State", how="inner")
joined.select("State", "2014 Population estimate", "Median_Age").show(10, False)


# Module 4: SQL & Temp Views
# Problem 6: Run SQL

df.createOrReplaceTempView("geo")

spark.sql("""
SELECT State, COUNT(*) AS county_count, ROUND(AVG(`2014 Population estimate`),2) AS avg_pop
FROM geo
GROUP BY State
ORDER BY avg_pop DESC
""").show(10, False)


# Module 6: Performance Tuning
# Problem 8: Repartition & caching

df_repart = df.repartition(4, "State")  # distribute by State
df_repart.cache()
print("Cached row count:", df_repart.count())


# Module 7: Data Sources
# Problem 9: Save as Parquet & Reload

df.write.mode("overwrite").parquet("/tmp/geo_parquet")

df_parquet = spark.read.parquet("/tmp/geo_parquet")
df_parquet.show(5)


# Module 8: RDD Basics
# Problem 10: Convert to RDD and run map/filter

rdd = df.rdd.map(lambda row: (row.State, row["2014 Population estimate"]))
filtered = rdd.filter(lambda x: x[1] and x[1] > 200000).take(5)
print(filtered)


# Module 9: Streaming (intro-level, often in exams)
# Problem 11: Streaming word count (using socket)

# from pyspark.sql.functions import explode, split
#
# lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
#
# words = lines.select(explode(split(lines.value, " ")).alias("word"))
#
# word_counts = words.groupBy("word").count()
#
# query = word_counts.writeStream.outputMode("complete").format("console").start()

#Demo - Truncate

data = [("Adhithi", "This is a very long address in Bengaluru, India"),
        ("Vandana", "Chennai, TN, India")]

columns = ["Name", "Address"]
df = spark.createDataFrame(data, columns)

df.show(2, truncate=True)   # default → cuts long strings at 20 chars
df.show(2, truncate=False)  # full content

