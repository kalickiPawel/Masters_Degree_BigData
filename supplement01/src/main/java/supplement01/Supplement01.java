package supplement01;

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

import static java.util.Arrays.*;
import static org.apache.spark.sql.functions.*;

public class Supplement01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> pages = spark.read().format("xml").option("rowTag", "page").load("supplement01/Wikipedia-20200603210232.xml");
//        pages.show();
//        pages.printSchema();
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
        graphFrame.pageRank().maxIter(10).run();

//        List<MyEdge> edgeList = asList(
//                new MyEdge(1L,2L,"a"), new MyEdge(1L,3L,"a"),
//                new MyEdge(1L,4L,"a"), new MyEdge(2L,3L,"b"),
//                new MyEdge(3L,4L,"a"), new MyEdge(2L,4L,"b")
//        );
//
//        Dataset<Row> edges = spark.createDataFrame(edgeList, MyEdge.class);
//
//        List<MyNode> nodeList = asList(
//                new MyNode(1L, "Janek"), new MyNode(2L, "Bolek"),
//                new MyNode(3L, "Lolek"), new MyNode(4L,"Tola")
//        );
//
//        Dataset<Row> vertices = spark.createDataFrame(nodeList, MyNode.class);
//
//        GraphFrame graph1 = GraphFrame.fromEdges(edges);
//        graph1.pageRank().maxIter(20).resetProbability(0.15).run().vertices().show();
//
//        GraphFrame graph2 = GraphFrame.apply(vertices, edges);
//        graph2.pageRank().maxIter(10).resetProbability(0.15).run().vertices().show();

        sc.stop();
        sc.close();
    }
}
