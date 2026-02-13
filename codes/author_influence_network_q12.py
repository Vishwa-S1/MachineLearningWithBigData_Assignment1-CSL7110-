from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, year, abs, count, regexp_extract
from pyspark.sql.types import StringType, DateType, StructType, StructField
import re
from datetime import datetime

spark = SparkSession.builder \
    .appName("AuthorInfluenceNetwork") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

def clean_text(text):
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

def extract_author(text):
    pattern = r"Author:\s*([^\n,]+)"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return clean_text(match.group(1))
    return None

def extract_date(text):
    pattern = r"Release Date:\s*([^\n]+)"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        date_str = clean_text(match.group(1))
        try:
            date = datetime.strptime(date_str, "%B %d, %Y")
            return date.strftime("%Y-%m-%d")
        except:
            return None
    return None

extract_author_udf = udf(extract_author, StringType())
extract_date_udf = udf(extract_date, StringType())

print("=== Author Influence Network Analysis ===")

# Read and process books
books_df = spark.read.option("wholeFile", True) \
    .text("C:\\Users\\vishwa\\OneDrive\\Desktop\\MLBD\\Assignment\\Dataset\\D184MB\\200.txt")
    

print(f"Total books loaded: {books_df.count()}")

# Extract metadata with validation
meta_df = books_df \
    .withColumn("author", extract_author_udf(col("value"))) \
    .withColumn("release_date", extract_date_udf(col("value"))) \
    .where(col("author").isNotNull() & col("release_date").isNotNull())

print("\nSample of extracted metadata:")
meta_df.select("author", "release_date").show(5, truncate=False)

# Process dates
meta_df = meta_df \
    .withColumn("release_date", col("release_date").cast(DateType())) \
    .withColumn("release_year", year(col("release_date"))) \
    .select("author", "release_year") \
    .distinct() \
    .cache()

valid_count = meta_df.count()
print(f"\nValid records: {valid_count}")

if valid_count == 0:
    print("Error: No valid metadata found!")
    spark.stop()
    exit(1)

# Create influence network
YEAR_WINDOW = 5
influence_df = meta_df.alias("a1").crossJoin(meta_df.alias("a2")) \
    .where(
        (col("a1.author") != col("a2.author")) & 
        (abs(col("a1.release_year") - col("a2.release_year")) <= YEAR_WINDOW)
    ) \
    .select(
        col("a1.author").alias("author1"),
        col("a2.author").alias("author2"),
        col("a1.release_year").alias("year1"),
        col("a2.release_year").alias("year2")
    )

connections = influence_df.count()
print(f"\nInfluence connections found: {connections}")

if connections > 0:
    print("\nSample connections:")
    influence_df.show(5)

    # Calculate network metrics
    in_degree = influence_df.groupBy("author2") \
        .agg(count("*").alias("in_degree"))
    
    out_degree = influence_df.groupBy("author1") \
        .agg(count("*").alias("out_degree"))
    
    metrics = in_degree.join(out_degree, 
                           in_degree.author2 == out_degree.author1, 
                           "outer") \
        .select(
            coalesce(in_degree.author2, out_degree.author1).alias("author"),
            coalesce(col("in_degree"), lit(0)).alias("in_degree"),
            coalesce(col("out_degree"), lit(0)).alias("out_degree")
        )
    
    print("\nTop 5 Most Influenced Authors:")
    metrics.orderBy(col("in_degree").desc()).show(5)
    
    print("\nTop 5 Most Influential Authors:")
    metrics.orderBy(col("out_degree").desc()).show(5)

spark.stop()
