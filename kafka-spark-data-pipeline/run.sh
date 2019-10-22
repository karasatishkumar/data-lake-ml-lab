#!/bin/bash


echo '#################################';
echo '# RUNNING THE SPARK APPLICATION #';
echo '#################################';

cd /app

chmod 777 *

/usr/spark-2.4.1/bin/spark-submit --class org.ns2.spark.WordCounter --master spark://master:7077 kafka-spark-data-pipeline-1.0-SNAPSHOT.jar input.txt

