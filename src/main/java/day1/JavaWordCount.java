package day1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWordCounter");
        conf.setMaster("local");
        JavaSparkContext jconf = new JavaSparkContext(conf);

        JavaRDD<String> lines = jconf.textFile("l-gfs.txt");
        JavaRDD<String> words = lines.flatMap(v -> Arrays.asList(v.split("")).iterator());
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(v->new Tuple2(v, 1));
        JavaPairRDD<String, Integer> reduceds = wordAndOne.reduceByKey((v1, v2) -> v1 + v2);
        // 因为Java里面只有SortBy value的选项，所以我们得先将Key和Value调换顺序，这样就可以用sortByKey进行排序了
        JavaPairRDD<Integer, String> reverted = reduceds.mapToPair(v->v.swap());
        JavaPairRDD<Integer, String> sorted = reverted.sortByKey(false);
        JavaPairRDD<String, Integer> realSorted = sorted.mapToPair(v->v.swap());
        realSorted.saveAsTextFile("output");

    }

}
