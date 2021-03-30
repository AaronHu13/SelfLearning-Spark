package day1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class JavaWordCount {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName("JavaWordCounter");
//        conf.setMaster("local");
//        JavaSparkContext jconf = new JavaSparkContext(conf);
//
////        JavaRDD<String> lines = jconf.textFile("l-gfs.txt");
////        JavaRDD<String> words = lines.flatMap(v -> Arrays.asList(v.split("")).iterator());
////        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(v->new Tuple2(v, 1));
////        JavaPairRDD<String, Integer> reduceds = wordAndOne.reduceByKey((v1, v2) -> v1 + v2);
////        // 因为Java里面只有SortBy value的选项，所以我们得先将Key和Value调换顺序，这样就可以用sortByKey进行排序了
////        JavaPairRDD<Integer, String> reverted = reduceds.mapToPair(v->v.swap());
////        JavaPairRDD<Integer, String> sorted = reverted.sortByKey(false);
////        JavaPairRDD<String, Integer> realSorted = sorted.mapToPair(v->v.swap());
////        realSorted.saveAsTextFile("output");
//
//        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
//        JavaRDD<Integer> javaRDD = jconf.parallelize(data,3);
//
////获得分区ID
//        JavaRDD<String> partitionRDD = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
//                LinkedList<String> linkedList = new LinkedList<String>();
//                while(v2.hasNext()){
//                    linkedList.add(v1 + "=" + v2.next());
//                }
//                return linkedList.iterator();
//            }
//        },false);
////        Iterator<String> it = partitionRDD.collect().iterator();
//        javaRDD.foreachPartition(it -> {
//                while(it.hasNext())
//                    System.out.print(it.next() + "      ");
//                System.out.println("\n___________end_________________");
//        });

        /*
        javaRDD.foreachPartition(new VoidFunction<Iterator<Integer>>() {
            @Override
            public void call(Iterator<Integer> integerIterator) throws Exception {
                System.out.println("___________begin_______________");
                while(integerIterator.hasNext())
                    System.out.print(integerIterator.next() + "      ");
                System.out.println("\n___________end_________________");
            }
        });*/

    }

}
