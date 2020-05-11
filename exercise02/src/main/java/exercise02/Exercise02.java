package exercise02;

// import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Exercise02 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// JavaRDD<String> textFile =
		// sc.textFile("file:///home/maria_dev/ratings.csv"); // sciezka bezwgledna

		JavaRDD<String> textFile = sc.textFile("../data/ratings.csv");
		JavaRDD<String[]> tablice = textFile.map(linia -> linia.split(","));

		JavaPairRDD<String, Integer> tabliceZJedynka = tablice.mapToPair(tablica -> new Tuple2<>(tablica[1], 1));
		JavaPairRDD<String, Integer> tabliceZLicznikiem = tabliceZJedynka.reduceByKey((a, b) -> a + b);

		// alternatywa dla swap:
		// JavaPairRDD<Integer, String> drugaTabliceZJedynka = tabliceZLicznikiem
		// .mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1));

		JavaPairRDD<Integer, String> drugaTabliceZJedynka = tabliceZLicznikiem.mapToPair(t -> t.swap());
		JavaPairRDD<Integer, String> posortowaneTablice = drugaTabliceZJedynka.sortByKey(true);

		posortowaneTablice.foreach(el -> System.out.println(el));

		// System.out.println("Total words: " + counts.count());

		sc.stop();
		sc.close();
	}

}
