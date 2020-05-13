package exercise05;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class Exercise05 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// I removed headers from files before run this program

		JavaRDD<String> ratings = sc.textFile("../data/ratings.csv");
		JavaRDD<String> movies = sc.textFile("../data/movies.csv");

		JavaRDD<Rating> tr = ratings.map(line -> {
			String[] arr = line.split(",");
			return new Rating(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), Double.parseDouble(arr[2]));
		});

		ALS.train(tr.rdd(), 10, 10);

		sc.stop();
		sc.close();
	}

}
