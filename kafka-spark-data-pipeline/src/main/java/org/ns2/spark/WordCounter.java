package org.ns2.spark;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class WordCounter
{
    private static final Pattern SPACE = Pattern.compile(" ");

    private static final Logger logger = Logger.getLogger(WordCounter.class);

    public static void main (String[] args) throws InterruptedException
    {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.137.33.201:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("messages");

        SparkConf conf = new SparkConf().setAppName(WordCounter.class.getSimpleName()).setMaster(
            "local").set("spark.cassandra.connection.host", "cassandra").set(
            "spark.cassandra.auth.username",
            "cassandra").set("spark.cassandra.auth.password", "cassandra");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf,
            Durations.seconds(1));

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
            streamingContext,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaPairDStream<String, String> results = messages.mapToPair(record -> new Tuple2<>(record.key(),
            record.value()));

        JavaDStream<String> lines = results.map(tuple2 -> tuple2._2());

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+")).iterator());

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s,
            1)).reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.foreachRDD(javaRdd -> {
            Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
            for (String key : wordCountMap.keySet()) {
                List<Word> wordList = Arrays.asList(new Word(key, wordCountMap.get(key)));
                for (Word word : wordList) {
                    logger.error("######################### word : {}" + word);
                }
                JavaRDD<Word> rdd = streamingContext.sparkContext().parallelize(wordList);

                CassandraJavaUtil.javaFunctions(rdd).writerBuilder("vocabulary",
                    "words",
                    CassandraJavaUtil.mapToRow(Word.class)).saveToCassandra();
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
