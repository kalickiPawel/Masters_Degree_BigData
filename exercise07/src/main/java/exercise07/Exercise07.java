package exercise07;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class Exercise07 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
		JavaSparkContext sc = new JavaSparkContext(conf);

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> orders = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("exercise07/instacart/orders.csv");

		Dataset<Row> products = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("exercise07/instacart/products.csv");

		Dataset<Row> departments = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("exercise07/instacart/departments.csv");

		// Task 01
		orders.groupBy("order_hour_of_day")
				.count()
				.sort("order_hour_of_day")
				.show(Integer.MAX_VALUE, false);

		// Task 02
		List<DayOfWeekName> values = Arrays.asList(
				new DayOfWeekName(0,"niedziela"),
				new DayOfWeekName(1,"poniedziałek"),
				new DayOfWeekName(2,"wtorek"),
				new DayOfWeekName(3,"środa"),
				new DayOfWeekName(4,"czwartek"),
				new DayOfWeekName(5,"piątek"),
				new DayOfWeekName(6,"sobota")
		);

		Dataset<Row> df = spark.createDataset(values, Encoders.bean(DayOfWeekName.class)).toDF();

		orders.join(df, String.valueOf(df.col("order_dow")))
				.groupBy(col("day_name"))
				.count()
				.orderBy(col("count"))
				.show();

		// Task 03

	}
}
