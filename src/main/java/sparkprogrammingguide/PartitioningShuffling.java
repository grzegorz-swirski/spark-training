package sparkprogrammingguide;

import com.google.common.collect.Iterators;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PartitioningShuffling {

  public static void main(String[] args) {
    String exampleFilePath = args[0];
    SparkConf conf = new SparkConf().setAppName("Partitioning and shuffling").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<Integer, Integer>> onesData = exampleDataSameKeys();
    partitioningExample(sc, onesData);

    List<Tuple2<Integer, Integer>> data = exampleData();
    partitioningExample(sc, data);

    shuffleExample(sc, data);
  }

  private static void partitioningExample(
      JavaSparkContext sc, List<Tuple2<Integer, Integer>> data) {
    System.out.println("Data input: " + data);

    JavaRDD<Tuple2<Integer, Integer>> distData = sc.parallelize(data);
    System.out.println(
        String.format("No partitioner for now. partitioner = %s", distData.partitioner()));

    // if spark.default.parallelism isn't set default number of partitions = number of cores in the cluster
    System.out.println(String.format("Number of partitions = %s", distData.getNumPartitions()));

    // when no partitioner provided the data is distributed uniformly
    JavaPairRDD<Integer, Integer> distDataPairs = sc.parallelizePairs(data);
    printPartitions(
        distDataPairs, "Parallelized Java pairs, no partitioner. Data distributed uniformly.");

    // when providing hash partitioner (JavaPairRDD only), the data is distributed by keys hash codes
    distDataPairs = sc.parallelizePairs(data).partitionBy(new HashPartitioner(4));
    printPartitions(
        distDataPairs,
        "Parallelized Java pairs with hash partitioner. Data distrubuted by has codes.");
  }

  private static void shuffleExample(JavaSparkContext sc, List<Tuple2<Integer, Integer>> data) {
    JavaPairRDD<Integer, Integer> distData = sc.parallelizePairs(data);
    printPartitions(distData, "Paralellized data to be aggregated");

    System.out.println("Shuffle example. Data aggregated by key");
    // shuffle inside partition
    Function2<List<Integer>, Integer, List<Integer>> transformFunction =
        (i, j) -> {
          i.add(j);
          return i;
        };

    // shuffle between partitions, intermediate files are written, data is sent over the network between nodes
    Function2<List<Integer>, List<Integer>, List<Integer>> combineFunction =
        (i, j) -> {
          i.addAll(j);
          return i;
        };

    JavaPairRDD<Integer, List<Integer>> aggregated =
        distData.aggregateByKey(new ArrayList<Integer>(), transformFunction, combineFunction);
    printPartitions(aggregated, "Aggregated data partitions");
  }

  private static List<Tuple2<Integer, Integer>> exampleData() {
    List<Tuple2<Integer, Integer>> data = new ArrayList<>();
    for (int x = 1; x < 4; x++) {
      for (int y = 1; y < 5; y++) {
        data.add(new Tuple2<>(x, y));
      }
    }
    return data;
  }

  private static List<Tuple2<Integer, Integer>> exampleDataSameKeys() {
    List<Tuple2<Integer, Integer>> onesData = new ArrayList<>();
    for (int x = 1; x < 4; x++) {
      for (int y = 1; y < 3; y++) {
        onesData.add(new Tuple2<>(1, 1));
      }
    }
    return onesData;
  }

  private static void printPartitions(JavaPairRDD pairRdd, String message) {
    System.out.println(message);
    pairRdd.foreachPartition(x -> System.out.println("ITEMS: " + Iterators.toString((Iterator) x)));
  }
}
