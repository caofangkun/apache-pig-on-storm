package org.apache.storm;

public class LocalPOS {
  public static void main(String[] args) throws Exception {
    System.getProperties().put("execType", "local");
    POS.main(args);
  }
}
