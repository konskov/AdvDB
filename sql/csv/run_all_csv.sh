#!/bin/bash
echo "" > output_csv.out
for i in {1..5}
do
    echo "Query ${i}" >> output_csv.out
    spark-submit $(echo "q${i}-spark.py") >> output_csv.out
done