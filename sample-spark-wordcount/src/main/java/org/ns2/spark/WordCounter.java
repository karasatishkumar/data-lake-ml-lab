package org.ns2.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.regex.Pattern;

public class WordCounter
{
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main (String[] args)
    {
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }
        SparkSession spark = SparkSession.builder().appName(WordCounter.class.getSimpleName()).master(
            "spark://master:7077").getOrCreate();
        Dataset<String> df = spark.read().textFile(args[0]);
        FlatMapFunction<String, String> wordsExtractFunction = (line) -> {
            String lowerCaseLine = line.toLowerCase();
            return Arrays.asList(lowerCaseLine.split(" ")).iterator();
        };
        Dataset<String> words = df.flatMap(wordsExtractFunction, Encoders.STRING());
        Dataset<Row> countDS = words.groupBy("value").count();
        countDS.show(50, 0, false);
    }
}
