package supplement01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Supplement01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // JavaRDD<String> textFile =
        // sc.textFile("file:///home/maria_dev/shakespeare.txt"); // ścieżka bezwzględna
        JavaRDD<String> textFile = sc.textFile("data/shakespeare.txt"); // ścieżka względna
        JavaPairRDD<String, Integer> counts = textFile.flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
        counts.foreach(p -> System.out.println(p));
        System.out.println("Total words: " + counts.count());
        sc.stop();
        sc.close();
    }
}
