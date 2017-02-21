package marketingapp;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Product implements Serializable {

  String id;
  String name;
  String totalValue;
  String itemsNum;

  public Product(String values) {
    String[] parsed = values.split("#");
    id = parsed[0];
    name = parsed[1];
    totalValue = parsed[2];
    itemsNum = parsed[3];
  }
}
