Raw source> Bronze_Catalog> Schema> df> Deltatable

spark
# spark.sql(f"USE CATALOG {catalog_name}")
df= spark.table("workspace.deafult.movies")

%sql
CREATE CATALOG IF NOT EXISTS ecommerce;
USE CATALOG ecommerce;
CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

%sql
DROP CATALOG IF EXISTS ecommerce CASCADE;

df= spark.table(f"{catalog_name}.{schema_name}.{table_name}")
df.printSchema()
display(df)
df.count()
df.columns
df.show(5, truncate=False)
df.describe().show() or display(df.describe())
df.filter(F.col("customer_id").isNull()).count()

# Row, Column count
num_rows, num_cols= df.count(), len(df.columns)

# Column Creation
df=df.withColumn("profit", col("revenue") - col("budget"))
df=df.withColumn("source_file",F.col("_metadata.file_path")).withColumn("ingestion_date", F.current_timestamp())
df=df.withColumn("is_weekend", F.when(F.col("release_day").isin("Saturday","Sunday"), 1).otherwise(0))
df=df.withColumn("month_name", F.date_format(F.col("release_date"), "MMMM"))
display(df.limit(3))


# Renaming Column
df=df.withColumnRenamed("revenue", "total_revenue")

# Column selection
df_distict_genres= df.select("genre").distinct().show()
df_trimmed= df.select("title","imdb_rating","industry")
df_trimmed.show(3, truncate=False)

df_distint_industries= df.select("industry").distinct()
display(df_distint_industries)  

# Reorder Columns
desired_columns_order= ["title", "release_year", "studio", "genre", "imdb_rating", "budget", "revenue"]
df=df.select(desired_columns_order)


# Row Filtering
df_filtered= df_2002_2010=df.filter((df['release_year']>=2000) & (df['release_year']<=2010))
display(df_filtered)

from pyspark.sql.functions import col
df_2002_2010=df.filter((col('release_year')>=2000) & (col('release_year')<=2010))
df_2002_2010=df.filter((col('release_year').between(2000,2010)))

df_marvel=df.filter(col('studio')=='Marvel Studios')

# Removing trailing spaces
from pyspark.sql.functions import rtrim, col, regex_replace
df_silver=df.withColumn("studio", regex_replace(rtrim(col("studio"),r'[^A-Za-z0-9]'),''))

# Creating temp dataframe on fly
from pyspark.sql import functions as F, types as T
data=[
    ("2021-01-01", 32.0, 6.0, "Rain"),
    ("2021-01-02", 35.0, 5.0, "Sunny"),
    ("2021-01-03", 28.0, 7.0, "Cloudy")
]
schema="day STRING, max_temp FLOAT, min_temp FLOAT, conditions STRING"
df=spark.createDataFrame(data=data, schema=schema)

# Formatting day column
df=df.withColumn("day", F.to_date(F.col("day"), "yyyy-MM-dd"))
df=df.withColumn("day", F.to_date("day", "yyyy-MM-dd"))
df=df.withColumn("day", F.to_date(df["day"], "yyyy-MM-dd"))
display(df)

df.CreateOrReplaceTempView("weather_data")
df.CreateOrReplaceGlobalTempView("global_weather_data")

# Replacing full/formatting column values
anomalies={"Rainy":"Rain", "Sunnyy":"Sunny"}
df_replaced=df.replace(anomalies, subset=["conditions"])
df_replaced.distinct().show()

df_silver=df_bronze.withColumn("max_temp", F.regexp_replace((F.col("max_temp"), "C", "").castFloatType())).withColumn("min_temp", F.regexp_replace((F.col("min_temp"), "C", "").castFloatType()))
df_silver=df_bronze.withColumn("material", F.when(F.col("material")=="Plastic", "Polymer").when(F.col("material")=="Ruber", "Rubber").otherwise(F.col("material")))
df_silver=df_bronze.withColumn("category", F.when(F.col("category").isin("Electronics", "Appliances"), "Electro Appliances").otherwise(F.col("category")))
df_silver=df_bronze.withColumn("channel", F.when(F.col("channel")=="Online", "E-Commerce").otherwise(F.col("channel")))

# Converting negative values to positive
df_silver=df_bronze.withColumn("rating", F.when(F.col("rating").isNotNull(), F.abs(F.col("rating"))).otherwise(F.lit(None)))

# Typecasting
df=df.withColumn("item_id", F.col("item_id").cast(T.IntegerType()))
df=df.withColumn("price", F.col("price").cast("float"))

# Handling missing values
df_dropped=df.dropna(subset=["max_temp", "conditions"])
df_filled=df.fillna({"max_temp":df.agg(F.avg("max_temp")).first()[0], "conditions":"Unknown"})
df_imputed=df.fillna("Not Available", subset=["phone number"])
             
# Handling duplicates
df_duplicates=df_bronze.groupBy("date").count().filter("count>1")
df_deduped=df_bronze.dropDuplicates(["column1", "column2"])

# Uppercase/lowercase/Normalize
df_upper=df.withColumn("conditions_upper", F.upper(F.col("conditions")))
df_silver=df.withColumn("day_name", F.init(F.col("day_name")))

# Quarter and Week of Year Formatting
df_silver=df.withColumn("quarter", F.concat_ws("-", F.concat(F.lit("Q")), F.col("quarter"), F.lit("-"), F.col("year")))
df_silver=df.withColumn("week_of_year",F.concat_ws("-", F.concat(F.lit("Week")), F.col("week_of_year"), F.lit("-"), F.col("year")))

# Read csv
df=spark.read.csv("/Volumes/workspace/default/raw_data/orders.csv", header=True, inferSchema=True)
df=spark.read.option("header",True).option("inferSchema",True).csv("/Volumes/workspace/default/raw_data/orders.csv")
df=spark.read.option("header",True).option("delimeter",",").schema(brand_schema).csv(raw_data_path)

# Custom Schema
from PySpark.sql import functions as F, types as T
csv_schema= T.StructType([
    T.StructField("order_id", T.IntegerType(), True),   
    T.StructField("order_date", T.DateType(), True),   
      T.StructField("customer_id", T.IntegerType(), True),   
      T.StructField("order_status", T.StringType(), True)
])
df=spark.read.option("header",True).option("dateFormat", "yyyy-MM-dd").schema(csv_schema).csv("/Volumes/workspace/default/raw_data/orders.csv")
df=(spark.read.format("csv").option("header",True).option("dateFormat", "yyyy-MM-dd").schema(csv_schema).load("/Volumes/workspace/default/raw_data/orders.csv"))

# Write csv/parquet/delta
df.write.mode("overwrite").option("header",True).csv("/Volumes/workspace/default/processed_data/orders_csv")
df.write.mode("overwrite").format("csv").option("header",True).parquet("/Volumes/workspace/default/processed_data/orders")
df.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable("{catalog_name}.{schema_name}.{table_name}")

# SQL Queries
df=spark.sql("SELECT * FROM workspace.default.movies WHERE release_year BETWEEN 2000 AND 2010")
display(df)

df_weather=spark.sql("""
SELECT condition, ROUND(AVG(max_temp), 1) AS avg_max_temp
FROM weather_data
GROUP BY condition
ORDER BY AVG(max_temp) DESC 
""")
display(df_weather)

%sql
SELECT * FROM workspace.default.movies WHERE release_year BETWEEN 2000 AND 2010
SELECT condition, ROUND(AVG(max_temp), 1) AS avg_max_temp
FROM weather_data
GROUP BY condition
ORDER BY AVG(max_temp) DESC

# Joining DataFrames
customers=[(1, "Alice", "US", True),
           (2, "Bob", "UK", True),
           (3, "Cathy", "IN", None),
           (None, "David", "US", False)
           ]

orders=[(101,1,250.0,"US"), 
       (102,2,450.0,"UK"), 
       (103,3,150.0,"IN"), 
       (104,4,550.0,"US"), 
       (105,None,350.0,"CA")
       ]

schema_customers= T.StructType([
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("customer_country", T.StringType(), True),
    T.StructField("is_active", T.BooleanType(), True)
])

schema_orders= T.StructType([
    T.StructField("order_id", T.IntegerType(), True),
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("order_amount", T.DoubleType(), True),
    T.StructField("order_country", T.StringType(), True)
])

df_customers= spark.createDataFrame(data=customers, schema=schema_customers)
df_orders= spark.createDataFrame(data=orders, schema=schema_orders)

df_inner= df_orders.join(df_customers, on="customer_id", how="inner")  # outer, left, right, left_semi, left_anti, full, composite

# Aliasing
c,o = df_customers.alias("c"), df_orders.alias("o")
df_inner_clean=o.join(c, on=["customer_id","country"], how="inner").select("order_id", "customer_id", "amount", 
                                                               F.col("o.country").alias("shipping_country"), "name",
                                                               F.col("c.country").alias("customer_country"),"vip")
display(df_inner_clean)

# Explaining Query Plans
df_narrow=df.select("title", "release_year").filter(F.col("imdb_rating")>8.0)
df_narrow.explain(extended=True)
df_narrow.explain(mode="formatted")

# Repartition to avoid data movement
df_repartitioned= df.repartition(6, "studio")

# Coalesce to reduce number of partitions (eliminates task overhead, data movement, optimizes file writes)
df.coalesce(2)
df=df.withColumn("order_ts",F.coalesce(F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"), F.timestamp("order_ts", "MM/dd/yyyy HH:mm")))

# s3 csv as databricks table (SQL Editor)
CREATE TABLE workspace.default.company_stocks
USING delta
LOCATION 's3://cb-company-stocks-oo1/bronze/'
AS
SELECT *,
    current_timestamp() as ingestion_at,
    input_file_name() as source_file,
    uuid() as bronze_id
FROM csv.`s3://my-bucket/path/to/csvfiles/`
WITH (
  header = true
)

# Views
df_products.createOrReplaceTempView("temp_products_view")
df_products.createOrReplaceTempview("temp_brands_view")
display(spark.sql("SELECT * FROM temp_products_view limit 5"))

