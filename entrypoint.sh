#!/bin/bash
set -e
export HADOOP_CLASSPATH=$PWD/Data/opennlp-tools-1.9.3.jar
# ==========================================
#         DATASET SELECTION
# ==========================================

echo "Choose Dataset:"
echo "1) Small (Wikipedia-50-ARTICLES)"
echo "2) Full (Wikipedia-EN-20120601_ARTICLES)"
read -p "Enter choice [1-2]: " dataset_choice

if [ "$dataset_choice" == "1" ]; then
    DATASET="Data/Wikipedia-50-ARTICLES"
elif [ "$dataset_choice" == "2" ]; then
    DATASET="Data/Wikipedia-EN-20120601_ARTICLES"
else
    DATASET="Data/Wikipedia-50-ARTICLES"
fi

echo "Using dataset: $DATASET"

# ==========================================
#         RUN OPTION SELECTION
# ==========================================

echo ""
echo "Choose what to run:"
echo "1a | 1b | 1c | 1e | 2a | 2b | all"
read -p "Enter option: " option

# ==========================================
#         BUILD
# ==========================================
build() {
    echo ""
    echo "Building project..."
    mvn clean package
}

# ==========================================
#         CLEAN FUNCTION
# ==========================================

clean_outputs() {
    rm -rf output/output_1a output/output_2a output/output_2b
    for d in 1 2 3 4
    do
        rm -rf output/output_1b_d$d
        rm -rf output/output_1c_d$d
        rm -rf output/output_1e_d$d
    done
}

init() {
    build
    clean_outputs
}
# ==========================================
#         FUNCTIONS
# ==========================================

run_1a() {
    echo "Running 1a..."
    rm -rf output/output_1a
    time hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem1a.Top50Words "$DATASET" output/output_1a Data/stopwords.txt
}

run_1b() {
    echo "Running 1b..."
    for d in 1 2 3 4
    do
        echo "distance.d=$d"
        rm -rf output/output_1b_d$d
        time hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem1b.PairsCoOccurrence -D distance.d=$d "$DATASET" output/output_1b_d$d output/output_1a/part-r-00000
    done
}

run_1c() {
    echo "Running 1c..."
    for d in 1 2 3 4
    do
        echo "distance.d=$d"
        rm -rf output/output_1c_d$d
        time hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem1c.StripesCoOccurrence -D distance.d=$d "$DATASET" output/output_1c_d$d output/output_1a/part-r-00000
    done
}

run_1e() {
    echo "Running 1e..."
    for d in 1 2 3 4
    do
        # echo "distance.d=$d"
        # rm -rf output/output_1e_d$d
        # hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem1e.PairsClassAggregation -D distance.d=$d "$DATASET" output/output_1e_d$d output/output_1a/part-r-00000

        # 1. Pairs - Map-Class Aggregation
        echo "Running: Pairs Map-Class..."
        rm -rf output/output_1e_pairs_class_d$d
        time hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem1e.PairsClassAggregation -D distance.d=$d "$DATASET" output/output_1e_pairs_class_d$d output/output_1a/part-r-00000

        # 2. Pairs - Map-Function Aggregation
        echo "Running: Pairs Map-Function..."
        rm -rf output/output_1e_pairs_func_d$d
        time hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem1e.PairsFunctionAggregation -D distance.d=$d "$DATASET" output/output_1e_pairs_func_d$d output/output_1a/part-r-00000

        # 3. Stripes - Map-Class Aggregation
        echo "Running: Stripes Map-Class..."
        rm -rf output/output_1e_stripes_class_d$d
        time hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem1e.StripesClassAggregation -D distance.d=$d "$DATASET" output/output_1e_stripes_class_d$d output/output_1a/part-r-00000
    done
}

run_2a() {
    echo "Running 2a..."
    rm -rf output/output_2a
    time hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem2a.DocumentFrequency "$DATASET" output/output_2a Data/stopwords.txt
}

run_2b() {
    echo "Running 2b..."
    rm -rf output/output_2b
    time hadoop jar target/Assignment2NoSQL-1.0-SNAPSHOT.jar Problem2b.TFScore "$DATASET" output/output_2b Data/top100_df.tsv

    echo ""
    echo "Top 20 Results:"
    head -n 20 output/output_2b/part-r-00000
}

extract_top100() {
    echo "Extracting Top 100 DF..."
    time sort -k2,2nr output/output_2a/part-r-00000 | head -n 100 > Data/top100_df.tsv
}

# ==========================================
#         EXECUTION LOGIC
# ==========================================

case "$option" in

    1a)
        init
        run_1a
        ;;

    1b)
        init
        run_1a
        run_1b
        ;;

    1c)
        init
        run_1a
        run_1c
        ;;

    1e)
        init
        run_1a
        run_1e
        ;;

    2a)
        init
        run_2a
        ;;

    2b)
        init
        run_2a
        extract_top100
        run_2b
        ;;

    all)
        init
        run_1a
        run_1b
        run_1c
        run_1e
        run_2a
        extract_top100
        run_2b
        ;;

    *)
        echo "Invalid option"
        exit 1
        ;;

esac

echo ""
echo "Done!"