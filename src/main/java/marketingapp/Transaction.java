package marketingapp;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Transaction implements Serializable {

  private final String date;
  private final String time;
  private final String customerId;
  private final String itemId;
  private String itemsNum;
  private String totalPrice;

  public Transaction(String values) {
    String[] parsed = values.split("#");
    date = parsed[0];
    time = parsed[1];
    customerId = parsed[2];
    itemId = parsed[3];
    itemsNum = parsed[4];
    totalPrice = parsed[5];
  }

  public void giveDiscount(double discount) {
    double priceWithDiscount = Double.valueOf(totalPrice) * (1.0 - discount);
    totalPrice = String.valueOf(priceWithDiscount);
  }
}
