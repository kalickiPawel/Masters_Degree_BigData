package exercise06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class Exercise06 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");

		SparkSession spark = SparkSession
				.builder()
				.appName("Java spark SQL")
				.config(conf)
				.getOrCreate();

		Dataset<Row> movies_csv = spark.read()
				.option("header", true)
				.option("InferSchema", true)
				.csv("exercise06/data/movies.csv");

		Dataset<Row> movies_json = spark.read()
				.option("header", true)
				.option("InferSchema", true)
				.json("exercise06/data/movies.json");

		Dataset<Row> ratings = spark.read()
				.option("header", true)
				.option("InferSchema", true)
				.csv("exercise06/data/ratings.csv");

		movies_csv.createOrReplaceTempView("movie_csv");
		movies_json.createOrReplaceTempView("movie_json");
		ratings.createOrReplaceTempView("ratings");

		// Task01

//		movies_csv.show();
//		movies_csv.printSchema();
//
//		movies_json.show();
//		movies_json.printSchema();

		// Task02

//		Dataset<Row> result_1 = spark.sql(" SELECT * FROM movie_csv WHERE movieId > 500 ORDER BY movieId DESC");
//		Dataset<Row> result_2 = spark.sql(" SELECT * FROM movie_json WHERE movieId > 500 ORDER BY movieId DESC");
//
//		result_1.show();
//		result_2.show();

		// Task03

//		ratings.groupBy(col("movieId"))
//				.count()
//				.join(movies_csv, "movieId")
//				.select(col("title"),col("count").as("Ratings_counter"))
//				.show();

//		Dataset<Row> rating = spark.sql("SELECT movie_csv.title, COUNT(rating) AS ratings_counter FROM ratings INNER JOIN movie_csv ON ratings.movieId=movie_csv.movieId GROUP BY ratings.movieId, movie_csv.title");
//		rating.show();

		// Task04

//		Dataset<Movie> df = spark.read().json("exercise06/data/movies.json").as(Encoders.bean(Movie.class));
//		df.map((MapFunction<Movie, String>) movie -> movie.getTitle(), Encoders.STRING()).show();
//		df.show();

		// Task05

		Properties prop = new Properties();
		String appConfigPath = "exercise06/dbconfig.properties";
		try {
			prop.load(new FileInputStream(appConfigPath));
		} catch (IOException e) {
			e.printStackTrace();
		}

		Dataset<Row> ds1 = spark.read().jdbc(
				"jdbc:mysql://kukuruku.linuxpl.info:3306/kukuruku_bigdata",
				"movies",
				prop
		);

		Dataset<Row> ds2 = spark.read().jdbc(
				"jdbc:mysql://kukuruku.linuxpl.info:3306/kukuruku_bigdata",
				"ratings",
				prop
		);

		ds1.show();
		ds2.show();
	}
}
