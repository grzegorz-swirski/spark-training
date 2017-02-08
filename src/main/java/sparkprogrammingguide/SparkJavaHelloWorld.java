package sparkprogrammingguide;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SparkJavaHelloWorld {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Programming guide").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);



    printExample(1, "Simple map-reduce task");

    // initialize data and distribute to cluster nodes
    List<Integer> numData = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> distNumData = sc.parallelize(numData);

    // map
    JavaRDD<Integer> squares = distNumData.map(n -> n * n);
    System.out.println(String.format("Squares = %s", squares.collect()));
    // reduce
    int sumOfSquares = squares.reduce((a, b) -> a + b);
    System.out.println(String.format("Sum of squares = %s", sumOfSquares));



    printExample(2, "Working with key-value pairs");

    List<String> textData = Arrays.asList("a", "c", "a", "d", "c", "b", "a");
    System.out.println(String.format("Input data: %s", textData));
    JavaRDD<String> distTextData = sc.parallelize(textData);

    // map, shuffle and sort, reduce
    JavaPairRDD<String, Integer> pairs = distTextData.mapToPair(s -> new Tuple2<>(s, 1));
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey((value1, value2) -> value1 + value2);
    JavaPairRDD<String, Integer> sortedResult = counts.sortByKey(Comparator.naturalOrder());
    System.out.println(String.format("Resulting reduced, sorted, collected k-v pairs: %s",
            sortedResult.collect()));
    // note: if we collected it as a Map<K, V> instead of List<Tuple2<K, V>> it wouldn't be sorted anymore



    printExample(3, "Transformations: flatMap");
    // flatMap produces zero or more outputs from each input
    // takes FlatMapFunction which takes T and returns Iterator<R> (in this case T, U = Integer)
    JavaRDD<Integer> triplecated = distNumData.flatMap(n -> Arrays.asList(n, n, n).iterator());
    System.out.println(String.format("Flat triplecated data: %s", triplecated.collect()));

    // in comparison, map operation returns one output for each input. Can't dooo...
    // JavaRDD<Integer> mapDuplicatedDistNumData = distNumData.map(n -> SOME SINGLE OUTPUT);



    printExample(4, "Transformations: filter");
    List<Integer> evenNumbers = distNumData.filter(n -> n % 2 == 0).collect();
    System.out.println(String.format("Filtered even numbers: %s", evenNumbers));
  }

  private static void printExample(int exampleNumber, String description) {
    System.out.println(String.format("\nEXAMPLE %s: %s", exampleNumber, description));
  }
}
