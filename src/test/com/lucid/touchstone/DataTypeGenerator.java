package com.lucid.touchstone;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

public class DataTypeGenerator {
  private String[] words;
  private Random r = new Random();

  public DataTypeGenerator(File wordFile) throws FileNotFoundException {
    words = fileToString(wordFile).split("\n");
  }
  
  public DataTypeGenerator(String[] words) {
    this.words = words;
  }

  public String getRandomEmailAddress() {
    String randomWord = words[getRandomNumber(0, words.length)].replaceAll("'",
        "");
    String randomCompany = words[getRandomNumber(0, words.length)].replaceAll(
        "'", "");
    String[] domains = new String[] { "com", "org", "edu", "net" };
    String domain = domains[getRandomNumber(0, domains.length)];

    return randomWord + "@" + randomCompany + "." + domain;
  }

  public String getRandomPhoneNumber() {
    StringBuilder sb = new StringBuilder();
    sb.append(getRandomNumber(0, 9));
    sb.append("-");

    sb.append(getRandomNumberString(3, 0, 9));

    sb.append("-");

    sb.append(getRandomNumberString(3, 0, 9));

    sb.append("-");

    sb.append(getRandomNumberString(4, 0, 9));

    return sb.toString();
  }

  public String getRandomNumberString(int num, int low, int high) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < num; i++) {
      sb.append(getRandomNumber(low, high));
    }
    return sb.toString();
  }

  public String getRandomWord() {
    String randomWord = words[getRandomNumber(0, words.length)];

    return randomWord;
  }

  public String getRandomWords(int num) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < num; i++) {
      String randomWord = words[getRandomNumber(0, words.length)];
      if (sb.length() != 0) {
        sb.append(" ");
      }
      sb.append(randomWord);
    }
    return sb.toString();

  }

  public int getRandomNumber(final int low, final int high) {
    int randInt = (Math.abs(r.nextInt()) % (high - low)) + low;

    return randInt;
  }

  public String fileToString(File file) throws FileNotFoundException {

    FileInputStream fis = null;

    fis = new FileInputStream(file);

    InputStreamReader isr = new InputStreamReader(fis);

    BufferedReader br = new BufferedReader(isr);
    StringBuffer sb = new StringBuffer();

    try {
      String line = null;

      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
    } catch (final Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      try {
        fis.close();
      } catch (final Exception ex) {
      }
      try {
        br.close();
      } catch (IOException e) {

      }
      try {
        isr.close();
      } catch (Exception e) {

      }
    }

    return sb.toString();
  }
}
