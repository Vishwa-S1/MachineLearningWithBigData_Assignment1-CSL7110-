from pyspark.sql import SparkSession
from pyspark.sql.functions import (input_file_name, col, udf, broadcast, 
                                 regexp_replace, regexp_extract)
from pyspark.sql.types import StringType, FloatType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
import re
from numpy import dot
from numpy.linalg import norm
import os
import glob

# Spark Configuration
spark = SparkSession.builder \
    .appName("TFIDF_BookSimilarity") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.executor.cores", "4") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def clean_text(text):
    header_splits = re.split(r"\*\*\*\s*START OF", text, flags=re.IGNORECASE)
    text = header_splits[1] if len(header_splits) > 1 else header_splits[0]
    footer_splits = re.split(r"\*\*\*\s*END OF", text, flags=re.IGNORECASE)
    text = footer_splits[0]
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

# File Validation and Reading
input_path = "C:\\Users\\vishwa\\OneDrive\\Desktop\\MLBD\\Assignment\\Dataset\\D184MB"
print(f"\nDebug: Checking path: {input_path}")
try:
    files = glob.glob(f"{input_path}/*.txt")
    print(f"Found {len(files)} text files")
    print("Sample files:", files[:3])
except Exception as e:
    print(f"Error accessing directory: {str(e)}")
    exit(1)

# Read and Process Books
clean_text_udf = udf(clean_text, StringType())
books_df = spark.read.option("wholeFile", True) \
    .text(f"{input_path}/*.txt") \
    .withColumn("original_name", input_file_name()) \
    .withColumn("file_name", regexp_replace(input_file_name(), "^.*/", "")) \
    .repartition(8)

print("\nDebug: Initial DataFrame:")
books_df.select("file_name").show(5, truncate=False)
print(f"Total files loaded: {books_df.select('file_name').distinct().count()}")

# Clean and Transform Text
books_clean_df = books_df.withColumn("clean_text", clean_text_udf(col("value"))).cache()
print(f"Cleaned {books_clean_df.count()} books")

# Text Processing Pipeline
tokenizer = Tokenizer(inputCol="clean_text", outputCol="words_raw")
remover = StopWordsRemover(inputCol="words_raw", outputCol="words")
cv = CountVectorizer(inputCol="words", outputCol="tf_features")
idf = IDF(inputCol="tf_features", outputCol="tfidf_features")

# Transform Pipeline
books_tokenized = tokenizer.transform(books_clean_df)
books_filtered = remover.transform(books_tokenized)
cv_model = cv.fit(books_filtered)
books_tf = cv_model.transform(books_filtered)
idf_model = idf.fit(books_tf)
books_features = idf_model.transform(books_tf).select("file_name", "tfidf_features").cache()

def cosine_similarity(v1, v2):
    a = v1.toArray()
    b = v2.toArray()
    if norm(a) == 0.0 or norm(b) == 0.0:
        return float(0.0)
    return float(dot(a, b) / (norm(a) * norm(b)))

cosine_udf = udf(cosine_similarity, FloatType())

# Process Target Book
target_book = "10.txt"
print(f"\nDebug: Looking for target book: {target_book}")

target_df = books_features.filter(col("file_name") == target_book)
target_count = target_df.count()

if target_count == 0:
    print("\nAvailable files (sample):")
    books_features.select("file_name").distinct().show(10, truncate=False)
    raise Exception(f"Target book {target_book} not found!")

target_df = target_df.alias("target").cache()

# Batch Processing
BATCH_SIZE = 100
books_features = books_features.cache()
total_books = books_features.count()
num_batches = (total_books + BATCH_SIZE - 1) // BATCH_SIZE

try:
    for batch in range(num_batches):
        start_idx = batch * BATCH_SIZE
        end_idx = min((batch + 1) * BATCH_SIZE, total_books)
        
        print(f"\nProcessing batch {batch+1}/{num_batches} (rows {start_idx} to {end_idx})")
        
        current_batch = books_features \
            .filter(col("file_name") != target_book) \
            .limit(end_idx) \
            .subtract(books_features.limit(start_idx)) \
            .alias("others")
        
        batch_count = current_batch.count()
        print(f"Books in current batch: {batch_count}")
        
        if batch_count == 0:
            continue
            
        similarity_df = target_df.crossJoin(broadcast(current_batch)) \
            .select(
                col("target.file_name").alias("target_file"),
                col("others.file_name").alias("other_file"),
                cosine_udf(col("target.tfidf_features"), 
                          col("others.tfidf_features")).alias("cosine_sim")
            )
        
        print("\nTop Similar Books in Current Batch:")
        similarity_df.orderBy(col("cosine_sim").desc()).show(5, truncate=False)
        
        spark.catalog.clearCache()

except Exception as e:
    print(f"Error in processing: {str(e)}")
finally:
    spark.stop()
