package com.lucid.touchstone;

public class Batcher {
  private int size;

  private volatile int batchSize;

  int start_index = 0;

  int end_index = 0;

  boolean first = true;

  public Batcher(int size, int batchSize) {

    this.size = size;
    this.batchSize = batchSize;

    if (batchSize > size) {
      batchSize = size;
    }
    end_index = batchSize - 1;

    System.out.println("new batcher - startindex:" + start_index + " endindex:"
        + end_index + " batchsize:" + batchSize);
  }

  public int getEndIndex() {
    return end_index;
  }

  public int getStartIndex() {
    return start_index;
  }

  public boolean next() {
    if (first) {
      first = false;
      return true;
    } else {

      start_index = end_index + 1;
      end_index = end_index + batchSize + 1;
      if (end_index > size - 1) {
        end_index = size - 1;
      }
      if (start_index > end_index) {
        return false;
      }

      return true;
    }

  }

  public void setBatchSize(int size) {
    this.batchSize = size;
  }

}
