package exercise03;

// import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Exercise03 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// I removed headers from files before run this program

		JavaRDD<String> ratings = sc.textFile("ratings.csv");
		JavaRDD<String> movies = sc.textFile("movies.csv");

		JavaPairRDD<Integer, Double> sortedTabSums = ratings.map(linia -> linia.split(","))
				.mapToPair(tablica -> new Tuple2<>(tablica[1], tablica[2]))
				.mapToPair(f -> new Tuple2<>(f._1, Double.parseDouble(f._2))).reduceByKey((a, b) -> a + b)
				.mapToPair(f -> new Tuple2<>(Integer.parseInt(f._1), f._2)).sortByKey();

		JavaPairRDD<Integer, Integer> sortedTabCounts = ratings.map(linia -> linia.split(","))
				.mapToPair(tablica -> new Tuple2<>(tablica[1], 1)).reduceByKey((a, b) -> a + b)
				.mapToPair(f -> new Tuple2<>(Integer.parseInt(f._1), f._2)).sortByKey();

		JavaPairRDD<String, Double> tabWithAvgStringKey = sortedTabSums.join(sortedTabCounts)
				.mapToPair(t -> new Tuple2<>(t._1, t._2._1 / t._2._2))
				.mapToPair(f -> new Tuple2<>(String.valueOf(f._1), f._2));

		JavaPairRDD<String, Double> result = movies.map(linia -> linia.split(","))
				.mapToPair(tab -> new Tuple2<>(tab[0], tab[1])).join(tabWithAvgStringKey)
				.mapToPair(tup -> new Tuple2<>(tup._2._2, tup._2._1)).sortByKey(false).mapToPair(f -> f.swap());

		result.foreach(el -> System.out.println(el));

		sc.stop();
		sc.close();
	}

}
