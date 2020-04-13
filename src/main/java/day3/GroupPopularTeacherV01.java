package day3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class GroupPopularTeacherV01 {
    public static void main(String[] args) {
        String input = "teacher.log";
        SparkConf conf = new SparkConf().setAppName("GroupPopularTeacher").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(input);
//        JavaRDD<Tuple2<String, String>> subjectAndTeacher = lines.map(line -> {
//            String[] splitRes = line.split("/");
//            String subject = splitRes[2].split("\\.")[0];
//            String teacher = splitRes[3];
//            return new Tuple2<String, String>(subject, teacher);
//        });
//        JavaPairRDD<Tuple2<String, String>, Integer> subjectAndTeacherAndOne = subjectAndTeacher.mapToPair(v -> new Tuple2<>(v, 1));
        JavaPairRDD<Tuple2<String, String>, Integer> subjectAndTeacherAndOne = lines.mapToPair(line -> {
            String[] splitRes = line.split("/");
            String subject = splitRes[2].split("\\.")[0];
            String teacher = splitRes[3];
            return new Tuple2<>(new Tuple2<>(subject, teacher), 1);
        });

        JavaPairRDD<Tuple2<String, String>, Integer> reduced = subjectAndTeacherAndOne.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<String, Iterable<Tuple2<Tuple2<String, String>, Integer>>> groupReduced = reduced.groupBy(v1 -> v1._1._1, 3);
        JavaPairRDD<Integer, Tuple2<String, String>> res = groupReduced.mapValues(it -> it.iterator().next()).mapToPair(v -> v._2.swap());
        List<Tuple2<Tuple2<String, String>, Integer>> finalRes = res.sortByKey().mapToPair(v -> v.swap()).take(3);
        System.out.println(finalRes.toString());
    }
}
