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
		// sc.textFile("file:///home/maria_dev/shakespeare.txt"); // �cie�ka bezwgl�dna
		// JavaRDD<String> textFile = sc.textFile("shakespeare.txt"); // �ciezka
		// wzgl�dna
		// JavaRDD<String> slowa = textFile.flatMap(linia ->
		// Arrays.asList(linia.split("[ ,.?!]")).iterator());
		// JavaPairRDD<String, Integer> slowaZJedynka = slowa.mapToPair(slowo -> new
		// Tuple2<>(slowo, 1));
		// JavaPairRDD<String, Integer> slowaZLicznikiem = slowaZJedynka.reduceByKey((a,
		// b) -> a + b);
		// slowaZLicznikiem.foreach(el -> System.out.println(el));

		JavaRDD<String> textFile = sc.textFile("ratings.csv");
		JavaRDD<String[]> tablice = textFile.map(linia -> linia.split(","));

		JavaPairRDD<String, Integer> tabliceZJedynka = tablice.mapToPair(tablica -> new Tuple2<>(tablica[1], 1));
		JavaPairRDD<String, Integer> tabliceZLicznikiem = tabliceZJedynka.reduceByKey((a, b) -> a + b);

		// JavaPairRDD<Integer, String> drugaTabliceZJedynka = tabliceZLicznikiem
		// .mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1));

		JavaPairRDD<Integer, String> drugaTabliceZJedynka = tabliceZLicznikiem.mapToPair(t -> t.swap());
		JavaPairRDD<Integer, String> posortowaneTablice = drugaTabliceZJedynka.sortByKey(true);

		posortowaneTablice.foreach(el -> System.out.println(el));

		// JavaPairRDD<String, Integer> counts = textFile.flatMap(s ->
		// Arrays.asList(s.split("[ ,]")).iterator())
		// .mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		// counts.foreach(p -> System.out.println(p));
		// System.out.println("Total words: " + counts.count());

		sc.stop();
		sc.close();
	}

}
