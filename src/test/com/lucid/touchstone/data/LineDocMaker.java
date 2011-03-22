package com.lucid.touchstone.data;

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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.rmi.server.UID;
import java.util.Random;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

/**
 * A DocMaker reading one line at a time as a Document from a single file. This
 * saves IO cost (over DirDocMaker) of recursing through a directory and opening
 * a new file for every document. It also re-uses its Document and Field
 * instance to improve indexing speed.
 * 
 * Config properties: docs.file=&lt;path to the file%gt;
 * doc.reuse.fields=true|false (default true)
 */
public class LineDocMaker extends BasicDocMaker {
  StreamProvider streamProvider;
  InputStream fileIS;
  BufferedReader fileIn;

  private static int READER_BUFFER_BYTES = 64 * 1024;

  private byte[] ipAddr;
  private String fileName;
  private CompressorStreamFactory csFactory = new CompressorStreamFactory();

  public interface StreamProvider {
    InputStream getStream() throws IOException;
  }
 

  public LineDocMaker(String fileName, StreamProvider stream) {
    try {
      InetAddress addr = InetAddress.getLocalHost();

      // Get IP Address
      ipAddr = addr.getAddress();
      this.fileName = fileName;
      this.streamProvider = stream;

      open();

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  protected void open() throws CompressorException {
    try {
      if (fileIn != null)
        fileIn.close();
      fileIS = streamProvider.getStream();
      if (fileName.endsWith(".bz2")) {
        // According to BZip2CompressorInputStream's code, it reads the first 
        // two file header chars ('B' and 'Z'). We only need to wrap the
        // underlying stream with a BufferedInputStream, since the code uses
        // the read() method exclusively.
        fileIS = new BufferedInputStream(fileIS, READER_BUFFER_BYTES);
        fileIS = csFactory.createCompressorInputStream("bzip2", fileIS);
      }
      // Wrap the stream with a BufferedReader for several reasons:
      // 1. We need the readLine() method.
      // 2. Even if bzip.compression is enabled, and is wrapped with
      // BufferedInputStream, wrapping with a buffer can still improve
      // performance, since the BIS buffer will be used to read from the
      // compressed stream, while the BR buffer will be used to read from the
      // uncompressed stream.
      fileIn = new BufferedReader(new InputStreamReader(fileIS, "UTF-8"), READER_BUFFER_BYTES);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  final static char SEP = '\t';

  public DocData setFields(String line) {
    // title <TAB> date <TAB> body <NEWLINE>
    final String title, date, body;
    
    int spot = line.indexOf(SEP);
    if (spot != -1) {
      title = line.substring(0, spot);
      int spot2 = line.indexOf(SEP, 1 + spot);
      if (spot2 != -1) {
        date = line.substring(1 + spot, spot2);
        body = line.substring(1 + spot2, getRandomNumber(1 + spot2 + 1, line
            .length()));
      } else
        date = body = "";
    } else
      title = date = body = "";

    String id = new UID().toString();

    DocData doc = new DocData(ipAddr + id, body, title, date);
    return doc;

  }

  private int getRandomNumber(final int low, final int high) {
    Random r = new Random();
    int randInt = (Math.abs(r.nextInt()) % (high - low)) + low;

    return randInt;
  }

  protected DocData getNextDocData() throws Exception {
    throw new RuntimeException("not implemented");
  }

  public DocData makeDocument() throws Exception {

    String line;
    synchronized (this) {
      while (true) {
        line = fileIn.readLine();
        if (line == null) {
          // Reset the file
          open();

        } else {
          break;
        }
      }
    }

    return setFields(line);

  }

  public int numUniqueTexts() {
    return -1;
  }

}
