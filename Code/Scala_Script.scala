import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

//  Create SparkSession
val spark = SparkSession.builder()
  .appName("PopulationPriceExample")
  .getOrCreate()

//   Load CSV file
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/databricks-datasets/samples/population-vs-price/data_geo.csv")

// Show schema
df.printSchema()

// ======== TRANSFORMATIONS ========

// 1. Select specific columns
val df1 = df.select("City", "State", "State Code", "2014 Population estimate")

// 2. Filter rows ( fix column name to match schema)
val df2 = df1.filter(col("2014 Population estimate") > 50000)

// 3. Drop duplicates
val df3 = df2.dropDuplicates(Seq("State", "County"))

// 4. Add new column
val df4 = df3.withColumn("Population_in_Lakhs", round(col("2014 Population estimate") / 100000, 2))

// 5. Rename column
val df5 = df4.withColumnRenamed("MedianHomeValue", "Median_House_Value")

// 6. Replace null values
val df6 = df5.na.fill(Map("Median_House_Value" -> 0))

// 7. GroupBy & aggregate
val df7 = df6.groupBy("State")
  .agg(
    avg("Median_House_Value").alias("Avg_House_Value"),
    count("*").alias("Total_Counties")
  )

// 8. Sort results
val df8 = df7.orderBy(col("Avg_House_Value").desc)

// 9. Conditional column
val df9 = df8.withColumn("High_Value_State", when(col("Avg_House_Value") > 300000, lit("Yes")).otherwise(lit("No")))

// 10. Limit rows
val df10 = df9.limit(10)

// ======== ACTIONS ========

// 11. Show top records
df10.show(false)

// 12. Collect results
val collectedData = df10.collect()

// 13. Count rows
val rowCount = df10.count()
println(s"Row Count: $rowCount")

// 14. Save to CSV (overwrite mode)
df10.write.mode("overwrite").option("header", "true").csv("/tmp/output_population_price")
df10.write.mode("overwrite").option("header", "true").csv("dbfs:/FileStore/output/output_population_price")

// 15. Take first 3 rows
val sampleData = df10.take(3)
println("Sample Data: " + sampleData.mkString(","))

// Stop SparkSession
spark.stop()
