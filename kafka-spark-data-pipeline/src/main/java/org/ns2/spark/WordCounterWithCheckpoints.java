package org.ns2.spark;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class WordCounterWithCheckpoints
{
    private static final Pattern SPACE = Pattern.compile(" ");

    private static final Logger logger = Logger.getLogger(WordCounterWithCheckpoints.class);

    public static JavaSparkContext sparkContext;

    public static void main (String[] args) throws InterruptedException
    {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.1.4:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("messages");

        SparkConf conf = new SparkConf().setAppName(WordCounterWithCheckpoints.class.getSimpleName()).setMaster(
            "spark://master:7077").set("spark.cassandra.connection.host", "cassandra").set(
            "spark.cassandra.auth.username",
            "cassandra").set("spark.cassandra.auth.password", "cassandra");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf,
            Durations.seconds(1));

        sparkContext = streamingContext.sparkContext();

        streamingContext.checkpoint("./.checkpoint");

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

        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> cumulativeWordCounts = wordCounts.mapWithState(
            StateSpec.function((word, one, state) -> {
            int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
            Tuple2<String, Integer> output = new Tuple2<>(word, sum);
            state.update(sum);
            return output;
        }));

        cumulativeWordCounts.foreachRDD(javaRdd -> {
            List<Tuple2<String, Integer>> wordCountList = javaRdd.collect();
            for (Tuple2<String, Integer> tuple : wordCountList) {
                List<Word> wordList = Arrays.asList(new Word(tuple._1, tuple._2));
                for (Word word : wordList) {
                    logger.error("########################################################## word : {}" + word);
                }
                JavaRDD<Word> rdd = sparkContext.parallelize(wordList);
                CassandraJavaUtil.javaFunctions(rdd).writerBuilder("vocabulary", "words", CassandraJavaUtil.mapToRow(Word.class))
                    .saveToCassandra();
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
