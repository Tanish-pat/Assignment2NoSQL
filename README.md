Here’s your content turned into a clean, professional **README.md**:

---

# 📘 Assignment 2: NoSQL Systems – MapReduce & Apache Hadoop

This repository contains the MapReduce implementation for **Assignment II: NoSQL Systems**. The project is divided into two main problems:

* **Co-occurring Word Matrix Generation** (Pairs, Stripes, and In-Mapper Combiner optimizations)
* **Indexing Documents** (Document Frequency and Term Frequency-based scoring using OpenNLP Porter Stemmer)

---

## 📂 Directory Structure

```
Assignment2NoSQL/
├── pom.xml
├── entrypoint.sh
├── Data/
│   ├── other files...
│   ├── Wikipedia-50-ARTICLES/
│   ├── Wikipedia-EN-20120601_ARTICLES/
│   └── opennlp-tools-1.9.3.jar
├── src/main/java/
│   ├── Problem1a/Top50Words.java
│   ├── Problem1b/PairsCoOccurrence.java
│   ├── Problem1c/StripesCoOccurrence.java
│   ├── Problem1e/PairsClassAggregation.java
│   ├── Problem1e/PairsFunctionAggregation.java
│   ├── Problem1e/StripesClassAggregation.java
│   ├── Problem2a/DocumentFrequency.java
│   └── Problem2b/TFScore.java
├── output/
├── screenshots/
└── other files...
```

---

## ⚙️ Prerequisites

Make sure the following are installed:

* **Java 8** (configured in `pom.xml`)
* **Apache Maven**
* **Apache Hadoop** (v3.3.6 compatible)
* **OpenNLP Tools**

  * Place `opennlp-tools-1.9.3.jar` inside the `Data/` directory

---

## 🚀 Build & Run Instructions

The entire workflow is automated using the `entrypoint.sh` script.

### 1. Make script executable

```bash
chmod +x entrypoint.sh
```

### 2. Run the script

```bash
./entrypoint.sh
```

### 3. Follow prompts

* **Dataset Selection**

  * Small dataset (50 articles)
  * Full Wikipedia dump

* **Execution Option**

  * Run a specific problem (`1a`, `1b`, `1c`, `1e`, `2a`, `2b`)
  * Or run **all** sequentially

### ✅ What the script handles automatically

* Maven build & packaging
* Hadoop job execution
* Distributed cache setup:

  * `stopwords.txt`
  * `top50.txt`
  * `top100_df.tsv`
* Iteration over window sizes: `d = {1,2,3,4}`
* Execution time tracking

---

## 🧠 Problem Details

### 🔹 Problem 1: Co-occurring Word Matrix

#### 1a: Top 50 Words

* Uses **Pairs approach**
* Filters stopwords using Distributed Cache
* Uses **single reducer** for global sorting
* Maintains ordering with `TreeMap`

---

#### 1b: Pairs Approach

* Builds co-occurrence matrix
* Evaluated for distances:

  ```
  d ∈ {1,2,3,4}
  ```

---

#### 1c: Stripes Approach

* Uses `MapWritable` to group neighbors
* Reduces network I/O compared to Pairs

---

#### 1e: Local Aggregation (In-Mapper Combining)

Optimizations implemented:

* **Map-Function Level**

  * Aggregation inside `map()` loop

* **Map-Class Level**

  * Uses class-level `HashMap`
  * Emits once in `cleanup()`

Implemented for both:

* Pairs
* Stripes

---

### 🔹 Problem 2: Document Indexing

#### 2a: Document Frequency (DF)

* Computes number of documents containing each term
* Uses **OpenNLP Porter Stemmer**
* Removes stopwords
* Extracts **Top 100 terms** for next stage

---

#### 2b: TF Score Computation

* Uses **Stripes approach**
* Computes Term Frequency (TF)
* Final score formula:

```
SCORE = TF × log10(10000 / DF + 1)
```

* Output format:

```
ID<TAB>TERM<TAB>SCORE
```

---

## 📝 Configuration Notes

### 🔹 OpenNLP Integration

To avoid `ClassNotFoundException`, the script sets:

```bash
export HADOOP_CLASSPATH=$PWD/Data/opennlp-tools-1.9.3.jar
```

---

### 🔹 Combiners

* Used in **Problem 1b (Pairs)**
* Helps reduce intermediate data transfer

---

## 📸 Outputs

* Results are stored in the `output/` directory
* Execution screenshots available in `screenshots/`

---

## 📌 Summary

This project demonstrates:

* Efficient MapReduce design patterns
* Performance optimization techniques:

  * Stripes vs Pairs
  * In-Mapper Combining
* Real-world text processing with Hadoop
* Scalable document indexing pipeline

---