package sparkprogrammingguide;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Dataset$;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class GithubPushCount {

  public static void main(String[] args) {

    String myFilesPath = "/home/grzegorz/my_files/spark_in_action_files/ch03";

    SparkConf conf = new SparkConf().setAppName("Github push counter").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    SparkSession sparkSession = SparkSession.builder()
        .config(conf)
        .getOrCreate();

    Dataset<Row> dataset = sparkSession.read().json(myFilesPath + "/github-archive/2015-03-01-0.json");
    Dataset<Row> pushEvents = dataset.filter("type = 'PushEvent'");
    pushEvents.show(10);

    // group by actor.login and reduce occurences (count)
    Dataset<Row> grouped = pushEvents.groupBy("actor.login").count();
    Dataset<Row> sorted = grouped.orderBy(grouped.col("count").desc());
    sorted.show(10);

    Set<String> employeesSet = new HashSet<>();
    Path employeesFilePath = Paths.get(myFilesPath, "ghEmployees.txt");
    try {
      Files.lines(employeesFilePath).forEach(l -> employeesSet.add(l));
    } catch (IOException e) {
      System.out.println(String.format("File in path %s does not exist.", employeesFilePath));
    }
    Broadcast<Set> employeesBroadcast = sc.broadcast(employeesSet);

    // TODO filter out logins not in employees set
//    Dataset<Row> filtered = sorted.filter();
//    filtered.show(10);
  }
}