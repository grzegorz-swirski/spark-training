package sparkprogrammingguide;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Marketing {

  private static final int DATE_IND = 0;
  private static final int TIME_IND = 1;
  private static final int CUSTOMER_ID_IND = 2;
  private static final int ITEM_ID_IND = 3;
  private static final int ITEMS_NUM_IND = 4;
  private static final int TOTAL_PRICE_IND = 5;

  public static void main(String[] args) {

    String productsFilePath = args[0];
    String transactionsFilePath = args[1];

    SparkConf conf = new SparkConf().setAppName("Marketing app").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // load transactions from file
    JavaRDD<String> transactionFileLines = sc.textFile(transactionsFilePath);
    JavaRDD<String[]> transactions = transactionFileLines.map(row -> row.split("#"));

    // create pair RDD with customer IDs as keys
    JavaPairRDD<Integer, String[]> transactionsByCustId =
        transactions.mapToPair(
            tData -> new Tuple2<>(Integer.valueOf(tData[CUSTOMER_ID_IND]), tData));

    // TASK 1: give 5% discount for customers who bought two or more products with ID 25
    transactionsByCustId =
        transactionsByCustId.mapValues(
            transaction -> {
              if (qualifiesForPromotion(transaction, 25, 2)) {
                System.out.println(
                    String.format("Before discount: %s", Arrays.toString(transaction)));
                transaction = discountTransaction(transaction, 0.05);
                System.out.println(
                    String.format("After discount:  %s\n", Arrays.toString(transaction)));
              }
              return transaction;
            });

    // TASK 2: add free toothbrush (ID 70) transaction for customers who bought 5 or more dictionaries (ID 81)
    transactionsByCustId =
        transactionsByCustId.flatMapValues(
            transaction -> {
              List<String[]> effectiveTransactions = new ArrayList<String[]>();
              effectiveTransactions.add(transaction);
              if (qualifiesForPromotion(transaction, 81, 5)) {
                effectiveTransactions.add(bonusItemForTransaction(transaction, 70));
              }
              return effectiveTransactions;
            });

    System.out.println(
        String.format("Transactions count with bonus items: %s", transactionsByCustId.count()));

    // TASK 3: find customer who made the most transactions
    JavaPairRDD<Integer, Integer> transactionsNumByCustId =
        transactionsByCustId.mapValues(tData -> 1).reduceByKey((v1, v2) -> v1 + v2);

    Tuple2<Integer, Integer> maxTransactionsNumCustomer =
        transactionsNumByCustId.max(
            (Comparator<Tuple2<Integer, Integer>> & Serializable) (t1, t2) -> t1._2() - t2._2());

    System.out.println(
        String.format(
            "Most transactions: customer ID=%s, transactions number=%s",
            maxTransactionsNumCustomer._1(), maxTransactionsNumCustomer._2()));

    // lookup all transactions of that customer and print them
    List<String[]> transactionsOfMaxTransCust =
        transactionsByCustId.lookup(maxTransactionsNumCustomer._1());
    transactionsOfMaxTransCust.stream().forEach(t -> System.out.println(Arrays.toString(t)));

    // TASK 4: find customer who spent the most money overall
    JavaPairRDD<Integer, Double> transactionsValueByCustId =
        transactionsByCustId
            .mapValues(v -> Double.valueOf(v[TOTAL_PRICE_IND]))
            .foldByKey(0.0, (price1, price2) -> price1 + price2);

    // find customer with max total value of transactions
    Tuple2<Integer, Double> maxTransactionsValueCustomer =
        transactionsValueByCustId.max(
            (Comparator<Tuple2<Integer, Double>> & Serializable)
                (t1, t2) -> Double.compare(t1._2(), t2._2()));

    System.out.println(
        String.format(
            "Highest transactions value: customer ID=%s, transactions value=%s",
            maxTransactionsValueCustomer._1(), maxTransactionsValueCustomer._2()));

    // TASK 5: find names of products with total value sold, sorted alphabetically
    JavaPairRDD<Integer, String[]> transactionsByProductId =
        transactions.mapToPair(
            values -> new Tuple2<Integer, String[]>(Integer.valueOf(values[ITEM_ID_IND]), values));

    JavaPairRDD<Integer, Double> transactionsValueByProductId =
        transactionsByProductId
            .mapValues(values -> Double.valueOf(values[TOTAL_PRICE_IND]))
            .reduceByKey((price1, price2) -> price1 + price2);

    JavaPairRDD<Integer, String[]> productsByProductId =
        sc.textFile(productsFilePath)
            .map(line -> line.split("#"))
            .mapToPair(values -> new Tuple2<Integer, String[]>(Integer.valueOf(values[0]), values));

    JavaPairRDD<Integer, Tuple2<Double, String[]>> transactionsValueAndProductsByProductId =
        transactionsValueByProductId.join(
            productsByProductId); // new RDD contains elements for which the key existed in both RDDs

    JavaPairRDD<Integer, Tuple2<Double, String[]>> transactionsValueAndProductsByProductIdSorted =
        sortValuesAndProductsByName(transactionsValueAndProductsByProductId);
    List<Tuple2<Integer, Tuple2<Double, String[]>>> sorted =
        transactionsValueAndProductsByProductIdSorted.collect();

    // TASK 6: find a list of products not sold yesterday
    JavaPairRDD<Integer, String[]> productsNotSoldByProductId =
        productsByProductId.subtractByKey(transactionsByProductId);

    System.out.println("\nProducts not sold yesterday");
    productsNotSoldByProductId.foreach(
        p -> System.out.println(String.format("Product: %s", Arrays.toString(p._2()))));

    // TASK 7: statistics about transactions per customer
  }

  private static boolean qualifiesForPromotion(
      String[] transaction, int productIdReq, int minItemsBoughtNumReq) {
    return (Integer.valueOf(transaction[ITEM_ID_IND]) == productIdReq)
        && (Integer.valueOf(transaction[ITEMS_NUM_IND]) >= minItemsBoughtNumReq);
  }

  private static String[] discountTransaction(String[] transaction, double discount) {
    double priceWithDiscount = Double.valueOf(transaction[TOTAL_PRICE_IND]) * (1.0 - discount);
    transaction[TOTAL_PRICE_IND] = String.valueOf(priceWithDiscount);
    return transaction;
  }

  private static String[] bonusItemForTransaction(String[] transaction, int bonusItemId) {
    String[] additionalTransaction = transaction.clone();
    additionalTransaction[ITEM_ID_IND] = String.valueOf(bonusItemId);
    additionalTransaction[ITEMS_NUM_IND] = "1";
    additionalTransaction[TOTAL_PRICE_IND] = "0.0";
    return additionalTransaction;
  }

  private static JavaPairRDD sortValuesAndProductsByName(
      JavaPairRDD<Integer, Tuple2<Double, String[]>> valueAndProductPairsById) {
    return valueAndProductPairsById
        .mapToPair(
            valueAndProductById -> {
              String productName = valueAndProductById._2()._2()[1];
              return new Tuple2<String, Tuple2<Integer, Tuple2<Double, String[]>>>(
                  productName, valueAndProductById);
            })
        .sortByKey()
        .mapToPair(
            pair -> {
              Integer productId = pair._2()._1();
              Tuple2<Double, String[]> valueAndProductById = pair._2()._2();
              return new Tuple2<Integer, Tuple2<Double, String[]>>(productId, valueAndProductById);
            });
  }
}
