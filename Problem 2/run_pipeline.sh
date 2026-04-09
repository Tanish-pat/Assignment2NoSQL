#!/bin/bash

# Exit immediately if any command fails
set -e

# ==========================================
#               CONFIGURATION
# ==========================================
# Toggle this variable to point to your desired dataset.
# Test dataset: Data/Wikipedia-50-ARTICLES
# Full dataset: Data/Wikipedia-EN-20120601_ARTICLES

DATASET="Data/Wikipedia-50-ARTICLES"
# DATASET="Data/Wikipedia-EN-20120601_ARTICLES"

# ==========================================

echo "=========================================="
echo "  1. Building Assignment 2 NoSQL Project  "
echo "=========================================="
mvn clean package

echo ""
echo "=========================================="
echo "  2. Setting up Environment & Cleaning    "
echo "=========================================="
export HADOOP_CLASSPATH=$(pwd)/Data/opennlp-tools-1.9.3.jar
echo "HADOOP_CLASSPATH set to: $HADOOP_CLASSPATH"
echo "Target Dataset: $DATASET"

# Remove old output directories so Hadoop doesn't crash
rm -rf output/output_2a output/output_2b
echo "Cleaned up old output directories."

echo ""
echo "=========================================="
echo "  3. Running Part 2a: Document Frequency  "
echo "=========================================="
hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem2a.DocumentFrequency "$DATASET" output/output_2a Data/stopwords.txt

echo ""
echo "=========================================="
echo "  4. Extracting Top 100 DF Terms          "
echo "=========================================="
cat output/output_2a/part-r-00000 | sort -k2,2nr | head -n 100 > Data/top100_df.tsv
echo "Top 100 terms extracted and saved to Data/top100_df.tsv"

echo ""
echo "=========================================="
echo "  5. Running Part 2b: TF Score (Stripes)  "
echo "=========================================="
hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem2b.TFScore "$DATASET" output/output_2b Data/top100_df.tsv

echo ""
echo "=========================================="
echo "  Pipeline Complete! Top 20 Results:      "Assignment2NoSQL/src/main/java/Problem2a
echo "=========================================="
# Show a preview of the final output
cat output/output_2b/part-r-00000 | head -n 20