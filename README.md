# ASSIGNMENT 1 – MACHINE LEARNING FOR BIG DATA  
**Course Code:** CSL7110  
**Name:** Vishwanath Singh  
**Roll No:** M24CSE030  

---

##  Assignment Overview

This assignment demonstrates the implementation of Big Data processing using:

- **Apache Hadoop** (MapReduce in Java)
- **Apache Spark** (PySpark)

### Work Covered

1. Hadoop HDFS operations  
2. MapReduce WordCount implementation  
3. Text processing with punctuation removal  
4. Large dataset processing (Project Gutenberg Dataset – D184MB)  
5. Metadata extraction using Spark  
6. TF-IDF based book similarity  
7. Author influence network construction  
8. Performance analysis and distributed execution  

All steps, execution outputs, and explanations are included in the report:  
**M24CSE030_CSL7110_Assignment1.pdf**

---

## Technologies Used

- Java (MapReduce)  
- Apache Hadoop (HDFS + MapReduce)  
- Apache Spark  
- PySpark  
- Python  
- Linux-based Hadoop Single Node Cluster  

---

## Assignment Structure

```
Java_MapReduce/
    WordCount_q1.java
    LyricsCount_q2.java
    PlaceHolder_q4.java
    TextAnalyzer_q5_q6.java
    WordCount_200txt_q7.java
    WordCount_execution_time_q9.java

PySpark/
    extract_all_metadata_q10.py
    tf_idf_book_similarity_q11.py
    author_influence_network_q12.py

Input Files/
    file01.txt
    file02.txt
    lyrics.txt
    punctuation.txt
    D184MB Dataset (Project Gutenberg books)

Output/
    output_200_q7.txt

Report/
    M24CSE030_CSL7110_Assignment1.pdf
```

---

# PART A – HADOOP (MAPREDUCE IN JAVA)

## Q1 – WordCount Example

Executed the default WordCount example to verify Hadoop setup.

**Input Files:**  
- file01.txt → "Hello World Bye World"  
- file02.txt → "Hello Hadoop Goodbye Hadoop"  

**Output:**  
```
Hello 2
World 2
Hadoop 2
Goodbye 1
Bye 1
```

---

## Q2 – Song Lyrics WordCount

Implemented custom Mapper and Reducer.

### Mapper:
- Input Key → LongWritable (byte offset)  
- Input Value → Text (line of text)  
- Output Key → Text (word)  
- Output Value → IntWritable (1)  

### Reducer:
- Input Key → Text (word)  
- Input Value → Iterable<IntWritable>  
- Output → Final word frequency count  

Punctuation removed using `String.replaceAll()`  
Tokenization performed using `StringTokenizer`  

---

## Q7 – WordCount on 200.txt (D184MB Dataset)

Processed a large Project Gutenberg dataset.  
Output stored in `output_200_q7.txt`  

---

# PART B – APACHE SPARK (PYSPARK)

## Q10 – Metadata Extraction

File: `extract_all_metadata_q10.py`

- Read full books using `wholeFile=True`  
- Extract Title, Release Date, Language, Encoding  
- Compute books per year  
- Find most common language  
- Calculate average title length  

---

## Q11 – TF-IDF Book Similarity

File: `tf_idf_book_similarity_q11.py`

Pipeline:
1. Clean Gutenberg headers and footers  
2. Tokenize text  
3. Remove stopwords  
4. Apply CountVectorizer  
5. Apply IDF  
6. Generate TF-IDF vectors  
7. Compute cosine similarity  

---

## Q12 – Author Influence Network

File: `author_influence_network_q12.py`

- Extract author and release date  
- Convert to release year  
- Create cross join of authors  
- Apply ±5 year window condition  
- Compute in-degree and out-degree  

---

# How To Run the assignment 

## Hadoop

```
javac -classpath `hadoop classpath` -d . WordCount_q1.java
jar -cvf WordCount.jar *
hadoop jar WordCount.jar WordCount input output
```

## Spark

```
spark-submit extract_all_metadata_q10.py
spark-submit tf_idf_book_similarity_q11.py
spark-submit author_influence_network_q12.py
```

---

#  Learning Outcomes

- Understanding of HDFS  
- MapReduce programming  
- Spark DAG execution  
- Distributed ML workflows  
- Large-scale text analytics  

---

#  Conclusion

This assignment demonstrates distributed storage and parallel computation using Hadoop and Spark. Dataset not uploaded due to size constraints.
Available locally.

