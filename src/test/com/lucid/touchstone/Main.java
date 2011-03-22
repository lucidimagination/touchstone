package com.lucid.touchstone;


public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2 || args.length > 4) {
      System.out.println("usage: wikifile numdocs url {numthreads}");

      return;
    }
    String wikiFile = args[0];
    int numDocs = Integer.parseInt(args[1]);
    String url = args[2];


    int numThreads = Runtime.getRuntime().availableProcessors() - 1;
    if (args.length > 3) {
      numThreads = Integer.parseInt(args[3]);;
    }
    
    WikiIndexer wi = new WikiIndexer();
    wi.go(url, wikiFile, numThreads, numDocs);
  }
  
}
