package labs.spark.pagerank;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;


public class PageRank {

    private static final Pattern SPACES = Pattern.compile("\\s+");
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("PageRank").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("data/pagerank.txt");


        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(
            new PairFunction<String, String, Iterable<String>>() {
                @Override
                public Tuple2<String, Iterable<String>> call(String s) throws Exception {
                    String[] parts = SPACES.split(s);
                    String[] neighbor = parts[2].split(",");
                    LinkedList<String> list = new LinkedList<String>();
                    for(String s1: neighbor){
                        list.add(s1);
                    }
                    return new Tuple2<String, Iterable<String>>(parts[0],list);
                }
            }
        ).cache();



        // Loads initial page rank from input.
        JavaPairRDD<String, Double> ranks = lines.mapToPair(
            new PairFunction<String, String, Double>(){
                @Override
                public Tuple2<String, Double> call(String s) throws Exception {
                    String[] parts = SPACES.split(s);
                    double initialRank = Double.parseDouble(parts[1]);
                    return new Tuple2<String, Double>(parts[0], initialRank);
                }
            }
        );


        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < 8 ; current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                .flatMapToPair(
                    new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
                        @Override
                        public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) throws Exception {
                            int urlCount = Iterables.size(s._1());
                            List<Tuple2<String, Double>> results = new ArrayList<>();

                            for (String url : s._1()) {
                                /**
                                 * Lab 6:
                                 * For each URL, calculate the PR contributed from its neighbor,
                                 * then add (url, PR) tuple into results list
                                 * */
                            }

                            return results;
                        }
                    }
                    );

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs
                .reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double v1, Double v2) throws Exception {
                        return v1 + v2;
                    }
                })
                .mapValues(new Function<Double, Double>() {
                    @Override
                    public Double call(Double sum) {
                        return 0.15 + sum * 0.85;
                    }
                });
        }


        // Collects all URL ranks and dump them to console.
        List<Tuple2<String, Double>> output = ranks.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
        }

        sc.stop();
    }



}
