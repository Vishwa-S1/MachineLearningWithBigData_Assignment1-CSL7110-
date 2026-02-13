from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, year, to_date, col, length, avg, regexp_extract
from pyspark.sql.functions import input_file_name, col, regexp_extract, regexp_replace, when, length, avg, lit
from pyspark.sql.functions import regexp_replace, when
from pyspark.sql.functions import trim
import re

spark = SparkSession.builder.appName("GutenbergMetadata").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read entire files as single records
books_df = spark.read.option("wholeFile", True).text("C:\\Users\\vishwa\\OneDrive\\Desktop\\MLBD\\Assignment\\Dataset\\D184MB\*")
books_df = books_df.withColumn("file_name", input_file_name())

title_extracted = regexp_extract(col("value"), r"(?i)Title:\s*(.+)", 1)
fallback_title = regexp_extract(col("value"), r"(?i)The Project Gutenberg E[E]?book of (.*?)[,\n]", 1)
final_title = when(title_extracted != "", trim(title_extracted)).otherwise(trim(fallback_title))
release_date_expr = regexp_extract(col("value"), r"(?i)Release Date:\s*([A-Za-z]+(?:\s+[0-9]{1,2})?,?\s+[0-9]{4})", 1)
language_expr = trim(regexp_replace(regexp_extract(col("value"), r"(?i)Language:\s*([^\n]+)", 1), r"\r", ""))
encoding_expr = trim(regexp_replace(regexp_extract(col("value"), r"(?i)Character set encoding:\s*([^\n]+)", 1), r"\r", ""))


metadata_df = books_df.select("file_name",final_title.alias("title"),trim(release_date_expr).alias("release_date"),language_expr.alias("language"),encoding_expr.alias("encoding")
)

metadata_df = metadata_df.withColumn("release_year", regexp_extract(col("release_date"), r"([0-9]{4})", 1))


books_per_year = metadata_df.groupBy("release_year").count().orderBy("release_year")
common_language = metadata_df.groupBy("language").count().orderBy(col("count").desc())
avg_title_length = metadata_df.select(avg(length("title")).alias("avg_title_length"))

# Show results
metadata_df.show(truncate=False)
books_per_year.show()
common_language.show(5)
avg_title_length.show()



