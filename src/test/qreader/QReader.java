package qreader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QReader {
  private static final Pattern LINE_READER = Pattern.compile(
      "^(.*?):\\s(.+?)(?:\\s#|$)", Pattern.MULTILINE);
  
  private static Map<String,List<String>> queries = new HashMap<String,List<String>>();
  
  public static void main(String[] args) throws IOException {
    File inputFile = new File(args[0]);
    File outFolder = new File(args[1]);
    BufferedReader reader = new BufferedReader(new FileReader(inputFile));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println("line:" + line);
        Matcher m = LINE_READER.matcher(line);
        while (m.find()) {
          String type = m.group(1);
          String query = m.group(2);
          List<String> qlist = queries.get(type);
          if (qlist == null) {
            qlist = new ArrayList<String>();
            queries.put(type, qlist);
          }
          qlist.add(query);
          System.out.println("type:" + type);
          System.out.println("query:" + query);
        }
      }
    } finally {
      reader.close();
    }
    System.out.println("mkdirs:" + outFolder);
    outFolder.mkdirs();
    Set<String> keys = queries.keySet();
    for (String key : keys) {
      writeType(key, new File(outFolder, key + ".txt"));
    }
  }

  private static void writeType(String type, File outFile) throws IOException {
    System.out.println("write file to:" + outFile);
    BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
    try {
      List<String> qlist = queries.get(type);
      for (String query : qlist) {
        writer.write(query + System.getProperty("line.separator"));
      }
    } finally {
      writer.close();
    }
    
  }
}
