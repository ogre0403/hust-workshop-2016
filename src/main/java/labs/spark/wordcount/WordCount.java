/**
 * Illustrates a wordcount in Java
 */
package labs.spark.wordcount;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    private static Logger logger = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        String inputFile = "data/wc.in";

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);

        // Split up into words.
        JavaRDD<String> words = input
            .flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String x) {
                        /**
                         * Lab 4:
                         * Use String.split() to generate String array, then
                         * use Arrays.asList() to create String iterable
                         * */
                        return null;
                    }
                }
            );

        // Transform into <word, one> pair.
        JavaPairRDD<String, Integer> word_one = words
            .mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        /**
                         * Lab 4:
                         * return (word, 1) tuple
                         * */
                        return null;
                    }
                }
            );
        List<Tuple2<String, Integer>> result;

        JavaPairRDD<String, Integer> counts_apporache_1 = word_one
            .reduceByKey(null
                /**
                 * Lab 4:
                 * create a Function2 anonymous class, and then
                 * implement Integer call(Integer v1, Integer v2) to sum up all values
                 * */
            );
        result = counts_apporache_1.collect();
        for(Tuple2 r : result)
            logger.info(r);

    }
}
