package exercise05;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

import scala.Tuple2;

public class Exercise05 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// I removed headers from files before run this program

		JavaRDD<String> ratings = sc.textFile("../data/ratings.csv");
		JavaRDD<String> movies = sc.textFile("../data/movies.csv");

		JavaPairRDD<String, Integer> ratingsCount = movies.map(linia -> linia.split(","))
				.mapToPair(tablica -> new Tuple2<>(tablica[1], 1)).reduceByKey((a, b) -> a + b)
				.mapToPair(tablica -> new Tuple2<>(tablica._1, tablica._2)).filter(x -> x._2 > 10);

		ratingsCount.foreach(el -> System.out.println(el));

		JavaRDD<Rating> tr = ratings.map(line -> {
			String[] arr = line.split(",");
			return new Rating(Integer.parseInt(arr[1]), Integer.parseInt(arr[0]), Double.parseDouble(arr[2]));
		});

		MatrixFactorizationModel model = ALS.train(tr.rdd(), 10, 10);

		Rating[] rec = model.recommendProducts(1, 3);

		for (Rating r : rec) {
			System.out.println(r);
		}

		sc.stop();
		sc.close();
	}

}
