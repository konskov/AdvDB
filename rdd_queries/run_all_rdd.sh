#!/bin/bash
echo "" > output_rdd.out
for i in {1..5}
do
    echo "Query ${i}" >> output_rdd.out
    spark-submit $(echo "q${i}.py") >> output_rdd.out
done