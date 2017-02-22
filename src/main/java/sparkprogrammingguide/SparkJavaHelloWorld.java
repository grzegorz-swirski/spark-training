package sparkprogrammingguide;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SparkJavaHelloWorld {

  private static int exampleCounter = 0;

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Programming guide").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    printExample("Simple map-reduce task");

    // initialize data and distribute to cluster nodes
    List<Integer> numData = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> distNumData = sc.parallelize(numData);

    // map
    JavaRDD<Integer> squares = distNumData.map(n -> n * n);
    System.out.println(String.format("Squares = %s", squares.collect()));
    // reduce
    int sumOfSquares = squares.reduce((a, b) -> a + b);
    System.out.println(String.format("Sum of squares = %s", sumOfSquares));

    printExample("Working with key-value pairs");

    List<String> textData = Arrays.asList("a", "c", "a", "d", "c", "b", "a", "b", "c", "a");
    System.out.println(String.format("Input data: %s", textData));
    JavaRDD<String> distTextData = sc.parallelize(textData);

    // map, shuffle and sort, reduce
    JavaPairRDD<String, Integer> pairs = distTextData.mapToPair(s -> new Tuple2<>(s, 1));
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey((value1, value2) -> value1 + value2);
    JavaPairRDD<String, Integer> sortedResult = counts.sortByKey(Comparator.naturalOrder());
    System.out.println(
        String.format(
            "Resulting reduced, sorted, collected k-v pairs: %s", sortedResult.collect()));
    // note: if we collected it as a Map<K, V> instead of List<Tuple2<K, V>> it wouldn't be sorted anymore

    printExample("Transformations: flatMap");
    // flatMap produces zero or more outputs from each input
    // takes FlatMapFunction which takes T and returns Iterator<R> (in this case T, U = Integer)
    JavaRDD<Integer> triplecated = distNumData.flatMap(n -> Arrays.asList(n, n, n).iterator());
    System.out.println(String.format("Flat triplecated data: %s", triplecated.collect()));

    // in comparison, map operation returns one output for each input. Can't dooo...
    // JavaRDD<Integer> mapDuplicatedDistNumData = distNumData.map(n -> SOME SINGLE OUTPUT);

    printExample("Transformations: filter");
    List<Integer> evenNumbers = distNumData.filter(n -> n % 2 == 0).collect();
    System.out.println(String.format("Filtered even numbers: %s", evenNumbers));

    pairs = distTextData.zip(distNumData);
    System.out.println("Input data = " + Arrays.toString(pairs.collect().toArray()));

    printExample("Transformations: reduceByKey"); // reduces the data to the same type as input
    System.out.println("no zero value passed as parameter in reduceByKey; multiplication");
    JavaPairRDD<String, Integer> reduced = pairs.reduceByKey((v1, v2) -> v1 * v2);
    System.out.println(Arrays.toString(reduced.collect().toArray()));

    printExample("Transformations: foldByKey"); // similar to reduce but zero value can be provided
    System.out.println("Zero value = 0; multiplication");
    JavaPairRDD<String, Integer> folded = pairs.foldByKey(0, (v1, v2) -> v1 * v2);
    System.out.println(Arrays.toString(folded.collect().toArray()));

    System.out.println("Zero value = 1; multiplication");
    folded = pairs.foldByKey(1, (v1, v2) -> v1 * v2);
    System.out.println(Arrays.toString(folded.collect().toArray()));

    printExample(
        "Transformations: aggregateByKey"); // aggregates data to a new type, uses transform and combine functions
    JavaPairRDD<String, List<Integer>> aggregated =
        pairs.aggregateByKey(
            new ArrayList<>(),
            (list, value) -> {
              list.add(value * 2); // additional operation can be performed in transform function
              return list;
            },
            (list1, list2) -> {
              list1.addAll(list2);
              return list1;
            });
    System.out.println(
        "Aggregated, collected data = " + Arrays.toString(aggregated.collect().toArray()));

    printExample("Transformations: groupByKey");
    JavaPairRDD<String, Iterable<Integer>> grouped = pairs.groupByKey();
    System.out.println("Grouped, collected data = " + Arrays.toString(grouped.collect().toArray()));

    printExample("Transformations: combineByKey");
    JavaPairRDD<String, List<Integer>> combined = pairs.combineByKey(i -> {
      List<Integer> initialList = new ArrayList<>();
      initialList.add(i);
      return initialList;
    }, (list, i) -> {
      list.add(i);
      return list;
    }, (list1, list2) -> {
      list1.addAll(list2);
      return list1;
    });
  }

  private static void printExample(String description) {
    System.out.println(String.format("\nEXAMPLE %s: %s", exampleCounter++, description));
  }
}
