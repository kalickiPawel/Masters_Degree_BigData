package exercise04;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Exercise04 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// I removed headers from files before run this program

		JavaRDD<String> ratings = sc.textFile("../data/ratings.csv");
		JavaRDD<String> movies = sc.textFile("../data/movies.csv");
		JavaRDD<String> tags = sc.textFile("../data/tags.csv");

		JavaPairRDD<String, Integer> titlesGenreCount = movies.map(s -> s.split("[,]"))
				.mapToPair(s -> new Tuple2<>(s[1], s[2])).mapToPair(s -> new Tuple2<>(s._1, s._2.split("[|]")))
				.mapToPair(s -> new Tuple2<>(s._1, s._2.length));

		// titlesGenreCount.foreach(p -> System.out.println(p));
		// -> uncomment for task 1

		JavaPairRDD<String, Integer> genresCounts = movies.map(s -> s.split("[,]"))
				.mapToPair(s -> new Tuple2<>(s[1], s[2])).map(s -> s._2.split("[|]"))
				.flatMap(s -> Arrays.asList(s).iterator()).mapToPair(word -> new Tuple2<>(word, 1))
				.reduceByKey((a, b) -> a + b).mapToPair(word -> new Tuple2<>(word._2, word._1)).sortByKey(false)
				.mapToPair(word -> new Tuple2<>(word._2, word._1));

		// genresCounts.foreach(p -> System.out.println(p));
		// -> uncomment for task 2

		JavaPairRDD<String, Integer> ratingPairsToCount = tags.map(s -> s.split("[,]")).mapToPair(line -> {
			try {
				return new Tuple2(line[1], 1);
			} catch (Exception e) {
				return new Tuple2(line[1], 1);
			}
		});
		JavaPairRDD<String, Integer> ratingsNumber = ratingPairsToCount.reduceByKey((a, b) -> a + b);
		JavaPairRDD<String, String> moviesTitles = movies.mapToPair(line -> {
			String[] tab = line.split(",");
			return new Tuple2(tab[0], tab[1]);
		});
		JavaPairRDD<String, Integer> titleRating = moviesTitles.join(ratingsNumber).mapToPair(el -> el._2);

		// titleRating.foreach(el -> System.out.println(el));
		// -> uncomment for task 3

		JavaPairRDD<String, Integer> finalPairs = titlesGenreCount.join(titleRating).mapToPair(tabRow -> {
			if (tabRow._2._1 < tabRow._2._2) {
				return new Tuple2(tabRow._1, 1);
			} else {
				return new Tuple2("", 0);
			}
		});

		JavaRDD lastFinal = finalPairs.filter(line -> !line._1.isEmpty()).map(x -> x._1);
		lastFinal.foreach(p -> System.out.println(p));

		sc.stop();
		sc.close();
	}

}
