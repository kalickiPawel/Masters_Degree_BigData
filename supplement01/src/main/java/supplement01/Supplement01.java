package supplement01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Supplement01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<MyEdge> edgeList = Arrays.asList(
                new MyEdge(1L,2L,"a"), new MyEdge(1L,3L,"a"),
                new MyEdge(1L,4L,"a"), new MyEdge(2L,3L,"b"),
                new MyEdge(3L,4L,"a"), new MyEdge(2L,4L,"b")
        );


        sc.stop();
        sc.close();
    }
}
