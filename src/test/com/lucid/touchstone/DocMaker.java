package com.lucid.touchstone;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



/**
 * Create documents for the test.
 * <br>Each call to makeDocument would create the next document.
 * When input is exhausted, the DocMaker iterates over the input again,
 * providing a source for unlimited number of documents,
 * though not all of them are unique. 
 */
public interface DocMaker {


  /** Create the next document. */
  public DocData makeDocument () throws Exception;
  
  /** Return how many real unique texts are available, 0 if not applicable. */ 
  public int numUniqueTexts();
  
  /** Return total bytes of all available unique texts, 0 if not applicable */ 
  public long numUniqueBytes();

  /** Return number of docs made since last reset. */
  public int getCount();

  /** Return total byte size of docs made since last reset. */
  public long getByteCount();

  /** Print some statistics on docs available/added/etc. */ 
  public void printDocStatistics();

}