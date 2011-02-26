package com.lucid.touchstone;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import com.lucid.touchstone.LineDocMaker.StreamProvider;

public class SolrDataPusher {
  private volatile int numAdded;

  private SolrServer server;

  protected ExecutorService pool;

  private int done;

  final private int numThreads;

  private AtomicLong totalTime = new AtomicLong(0);

  private DataTypeGenerator datagen = new DataTypeGenerator(new String[] {
      "test", "horse", "man" });

  public SolrDataPusher(SolrServer server, final int numThreads) {
    this.server = server;
    pool = Executors.newFixedThreadPool(numThreads);
    this.numThreads = numThreads;
  }

  public void generateDocs(final String sourceFile, final int numDocs)
      throws Exception {

    final DocMaker dm = new EnwikiDocMaker(sourceFile, new StreamProvider() {

      public InputStream getStream() throws FileNotFoundException {
        InputStream is = new FileInputStream(sourceFile);
        return is;
      }
    });

    int bs = numDocs / numThreads + 1;
    System.out.println("approx batch size:" + bs);
    final Batcher batcher = new Batcher(numDocs, bs);

    while (batcher.next()) {
      // System.out.println("batch start:" + batcher.start_index + " batch end:"
      // + batcher.end_index+ " " + Thread.currentThread().getId());
      final int num = batcher.end_index - batcher.start_index + 1;
      // System.out.println("start thread");
      pool.execute(new Runnable() {

        public void run() {
          System.out.println("batch num:" + num + " "
              + Thread.currentThread().getId());
          for (int i = 0; i < num; i++) {
            try {
              DocData doc = dm.makeDocument();
              SolrInputDocument solrDoc = new SolrInputDocument();
              solrDoc.addField("id", doc.id);
              solrDoc.addField("date", doc.date);
              solrDoc.addField("email", datagen.getRandomEmailAddress());
              solrDoc.addField("name", doc.name);
              solrDoc.addField("phone", doc.phone);
              solrDoc.addField("title", doc.title);
              solrDoc.addField("body", doc.body);
              long start = System.nanoTime();
              UpdateResponse resp = server.add(solrDoc);
              long end = System.nanoTime();
              totalTime.addAndGet(end - start);
              numAdded++;
            } catch (NoMoreDataException e) {
              System.out.println("no more data" + " "
                  + Thread.currentThread().getId());
              return;
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
          synchronized (SolrDataPusher.this) {
            done++;
            System.out.println("done: " + done + " out of " + numThreads
                + " threads");
            if (done == numThreads) {
              try {
                server.commit();
                System.out.println("approx num added:" + numAdded + "("
                    + numDocs + ")");
                ModifiableSolrParams params = new ModifiableSolrParams();
                params.add("q", "*:*");
                params.add("fl", "email");
                QueryResponse resp = server.query(params);
                System.out.println(resp);
              } catch (SolrServerException e) {
                e.printStackTrace();
              } catch (IOException e) {
                e.printStackTrace();
              }

            }
          }

        }
      });

    }
    shutdownAndAwaitTermination(pool);

  }

  public long getTotalIndexTime() {
    return totalTime.get();
  }

  /**
   * @param pool
   */
  protected void shutdownAndAwaitTermination(ExecutorService pool) {
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(10000, TimeUnit.SECONDS)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(60, TimeUnit.SECONDS))
          System.err.println("Pool did not terminate");
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

}
