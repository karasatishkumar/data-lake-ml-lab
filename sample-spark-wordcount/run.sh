#!/bin/bash


echo '#################################';
echo '# RUNNING THE SPARK APPLICATION #';
echo '#################################';

cd /app

chmod 777 *

/usr/spark-2.4.1/bin/spark-submit --class org.satish.spark.WordCounter --master spark://master:7077 sample-spark-wordcount-1.0-SNAPSHOT.jar input.txt

