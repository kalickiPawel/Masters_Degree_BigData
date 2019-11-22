package sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDemo {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sample Spark app");
		JavaSparkContext sparkContext = new JavaSparkContext();
		
	}

}
