package sparkprogrammingguide;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class GithubPushCount {

  public static void main(String[] args) {

    // GitHub archive for specific time period. Use eg. 'http://data.githubarchive.org/2015-03-01-{0..23}.json.gz'
    String ghArchivePath = args[0];
    String ghEmployeesPath = args[1];

    SparkConf conf = new SparkConf().setAppName("Github push counter").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    SparkSession spark = SparkSession.builder()
        .config(conf)
        .getOrCreate();

    Dataset<Row> dataset = spark.read().json(ghArchivePath);
    Dataset<Row> pushEvents = dataset.filter("type = 'PushEvent'");
    pushEvents.show(10);

    // group by actor.login and reduce occurences (count)
    Dataset<Row> grouped = pushEvents.groupBy("actor.login").count();
    Dataset<Row> sorted = grouped.orderBy(grouped.col("count").desc());
    sorted.show(10);

    // read employees file
    Set<String> employeesSet = new HashSet<>();
    Path employeesFilePath = Paths.get(ghEmployeesPath);
    try {
      Files.lines(employeesFilePath).forEach(employeesSet::add);
    } catch (IOException e) {
      System.err.println(String.format("File in path %s does not exist.", employeesFilePath));
      System.exit(-1);
    }

    // broadcast employees set to nodes
    Broadcast<Set> employeesBroadcast = sc.broadcast(employeesSet);

    // get employee pushes from all pushes
    Dataset<Row> filtered = sorted.filter((Row row) -> employeesBroadcast.getValue().contains(row.get(0)));
    filtered.show(10);
  }
}