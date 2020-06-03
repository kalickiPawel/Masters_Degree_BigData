package exercise08;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.*;

public class Exercise08 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> pages = spark.read().format("xml").option("rowTag", "page").load("exercise08/Wikipedia-20200603210232.xml");

        pages.printSchema();

        Dataset edges = pages.select(col("title"), col("revision.text._VALUE"))
                .flatMap((FlatMapFunction<Row, Relation>) page -> {
                    String title = page.getString(0);
                    String text = page.getString(1);
                    Pattern pattern = Pattern.compile("\\[\\[([.[^\\]]]+)\\]\\]");
                    Matcher matcher = pattern.matcher(text);
                    List<Relation> relations = new ArrayList<>();
                    while(matcher.find()){
                        String link = matcher.group(1);
                        relations.add(new Relation(title, link));
                    }
                    return relations.iterator();
                }, Encoders.bean(Relation.class));
        edges.show(Integer.MAX_VALUE, false);

        GraphFrame graphFrame = GraphFrame.fromEdges(edges);
        graphFrame.vertices().show();
        graphFrame.edges().show();
        graphFrame.pageRank().maxIter(5).resetProbability(0.15).run().vertices().show();

        sc.stop();
        sc.close();
    }
}
