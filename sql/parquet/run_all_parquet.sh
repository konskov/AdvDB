#!/bin/bash
echo "" > output_parquet.out
for i in {1..5}
do
    echo "Query ${i}" >> output_parquet.out
    spark-submit $(echo "q${i}-spark.py") >> output_parquet.out
done