#!/bin/bash


echo '#################################';
echo '# RUNNING THE SPARK APPLICATION #';
echo '#################################';

cd /app

mkdir .checkpoint

chmod 777 *

/usr/spark-2.4.1/bin/spark-submit --class org.ns2.spark.WordCounterWithCheckpoints --master spark://master:7077 kafka-spark-data-pipeline-1.0-SNAPSHOT.jar

