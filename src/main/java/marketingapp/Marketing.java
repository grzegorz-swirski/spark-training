package marketingapp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Marketing {

  public static void main(String[] args) {

    String productsFilePath = args[0];
    String transactionsFilePath = args[1];

    SparkConf conf = new SparkConf().setAppName("Marketing app").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // load transactions from file
    JavaRDD<String> transactionFileLines = sc.textFile(transactionsFilePath);
    JavaRDD<Transaction> transactions = transactionFileLines.map(Transaction::new);

    // create pair RDD with customer IDs as keys
    JavaPairRDD<Integer, Transaction> transactionsByCustomerId =
        transactions.mapToPair(
            tData -> new Tuple2<>(Integer.valueOf(tData.getCustomerId()), tData));

    // TASK 1: give 5% discount for customers who bought two or more products with ID 25
    transactionsByCustomerId =
        transactionsByCustomerId.mapValues(
            transaction -> {
              if (qualifiesForPromotion(transaction, 25, 2)) {
                transaction.giveDiscount(0.05);
              }
              return transaction;
            });

    // TASK 2: add free toothbrush (ID 70) transaction for customers who bought 5 or more dictionaries (ID 81)
    transactionsByCustomerId =
        transactionsByCustomerId.flatMapValues(
            transaction -> {
              List<Transaction> effectiveTransactions = new ArrayList<>();
              effectiveTransactions.add(transaction);
              if (qualifiesForPromotion(transaction, 81, 5)) {
                effectiveTransactions.add(bonusTransaction(transaction, 70));
              }
              return effectiveTransactions;
            });

    System.out.println(
        String.format(
            "\nTASK 2. Transactions count with bonus items: %s", transactionsByCustomerId.count()));

    // TASK 3: find customer who made the most transactions
    JavaPairRDD<Integer, Integer> transactionsNumByCustomerId =
        transactionsByCustomerId.mapValues(tData -> 1).reduceByKey((v1, v2) -> v1 + v2);

    Tuple2<Integer, Integer> maxTransactionsNumCustomer =
        transactionsNumByCustomerId.max(
            (Comparator<Tuple2<Integer, Integer>> & Serializable) (t1, t2) -> t1._2() - t2._2());

    System.out.println(
        String.format(
            "\nTASK 3. Most transactions: customer ID=%s, transactions number=%s",
            maxTransactionsNumCustomer._1(), maxTransactionsNumCustomer._2()));

    // TASK 4: find customer who spent the most money overall
    JavaPairRDD<Integer, Double> transactionsValueByCustomerId =
        transactionsByCustomerId
            .mapValues(v -> Double.valueOf(v.getTotalPrice()))
            .foldByKey(0.0, (price1, price2) -> price1 + price2);

    // find customer with max total value of transactions
    Tuple2<Integer, Double> maxTransactionsValueCustomer =
        transactionsValueByCustomerId.max(
            (Comparator<Tuple2<Integer, Double>> & Serializable)
                (t1, t2) -> Double.compare(t1._2(), t2._2()));

    System.out.println(
        String.format(
            "\nTASK 4. Highest transactions value: customer ID=%s, transactions value=%s",
            maxTransactionsValueCustomer._1(), maxTransactionsValueCustomer._2()));

    // TASK 5: find names of products with total value sold, sorted alphabetically
    JavaPairRDD<Integer, Transaction> transactionsByProductId =
        transactions.mapToPair(values -> new Tuple2<>(Integer.valueOf(values.getItemId()), values));

    JavaPairRDD<Integer, Double> transactionsValueByProductId =
        transactionsByProductId
            .mapValues(values -> Double.valueOf(values.getTotalPrice()))
            .reduceByKey((price1, price2) -> price1 + price2);

    JavaPairRDD<Integer, Product> productsByProductId =
        sc.textFile(productsFilePath)
            .map(Product::new)
            .mapToPair(p -> new Tuple2<>(Integer.valueOf(p.getId()), p));

    JavaPairRDD<Integer, Tuple2<Double, Product>> transactionsValueAndProductsByProductId =
        transactionsValueByProductId.join(
            productsByProductId); // new RDD contains elements for which the key existed in both RDDs

    JavaPairRDD<Integer, Tuple2<Double, Product>> transactionsValueAndProductsByProductIdSorted =
        sortValuesAndProductsByName(transactionsValueAndProductsByProductId);

    System.out.println("\nTASK 5. Products with total value sold, sorted by name, first 10");
    transactionsValueAndProductsByProductIdSorted
        .take(10)
        .forEach(
            t -> {
              System.out.println(String.format("Value: %s; Product: %s", t._2()._1(), t._2()._2()));
            });

    // TASK 6: find a list of products not sold yesterday
    JavaPairRDD<Integer, Product> productsNotSoldByProductId =
        productsByProductId.subtractByKey(transactionsByProductId);

    System.out.println("\nTASK 6. Products not sold yesterday");
    productsNotSoldByProductId.foreach(
        p -> System.out.println(String.format("Product: %s", p._2())));

    // TASK 7: statistics about transactions per customer
  }

  private static boolean qualifiesForPromotion(
      Transaction transaction, int productIdReq, int minItemsBoughtNumReq) {
    return (Integer.valueOf(transaction.getItemId()) == productIdReq)
        && (Integer.valueOf(transaction.getItemsNum()) >= minItemsBoughtNumReq);
  }

  private static Transaction bonusTransaction(Transaction transaction, int bonusItemId) {
    return new Transaction(
        transaction.getDate(),
        transaction.getTime(),
        transaction.getCustomerId(),
        String.valueOf(bonusItemId),
        "1",
        "0.0");
  }

  private static JavaPairRDD<Integer, Tuple2<Double, Product>> sortValuesAndProductsByName(
      JavaPairRDD<Integer, Tuple2<Double, Product>> valueAndProductPairsById) {
    return valueAndProductPairsById
        .mapToPair(
            valueAndProductById -> {
              String productName = valueAndProductById._2()._2().getName();
              return new Tuple2<>(productName, valueAndProductById);
            })
        .sortByKey()
        .mapToPair(
            pair -> {
              Integer productId = pair._2()._1();
              Tuple2<Double, Product> valueAndProductById = pair._2()._2();
              return new Tuple2<>(productId, valueAndProductById);
            });
  }
}
