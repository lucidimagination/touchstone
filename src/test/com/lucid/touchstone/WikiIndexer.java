package com.lucid.touchstone;

import java.util.logging.Level;
import java.util.logging.LogManager;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;

public class WikiIndexer {
  
  public void go(String url, String wikiFile, int numThreads, int numDocs) throws Exception {
    
    LogManager.getLogManager().getLogger("").setLevel(Level.WARNING);
    long start = System.nanoTime();
    // CoreContainer.Initializer initializer = new CoreContainer.Initializer();
    // CoreContainer coreContainer = initializer.initialize();
    
    // EmbeddedSolrServer server = new EmbeddedSolrServer(coreContainer, "");
    StreamingUpdateSolrServer server = new StreamingUpdateSolrServer(url, 1000, 3);
    //CommonsHttpSolrServer server = new CommonsHttpSolrServer(url);
    System.out.println("SolrServer Impl:" + server.getClass().getName());
    System.out.println("Using " + numThreads + " threads");
    SolrDataPusher solrdp = new SolrDataPusher(server, numThreads);
    
    solrdp.generateDocs(wikiFile, numDocs);
    
    long end = System.nanoTime();
    float time = (end - start) / 1000000000.0f;
    float timeInAddDoc = solrdp.getTotalIndexTime() / 1000000000.0f;
    System.out.println("Time in addDoc:" + timeInAddDoc);
    System.out.println("Total Time:" + time);
    System.out.println("Overall Speed:" + ((float) numDocs / time));
    
    // coreContainer.shutdown();
  }
  
}
