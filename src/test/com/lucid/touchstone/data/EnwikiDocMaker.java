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

import java.io.IOException;
import java.io.InputStream;
import java.rmi.server.UID;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.compressors.CompressorException;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * A {@link LineDocMaker} which reads the english wikipedia dump. You can read
 * the .bz2 file directly (it will be decompressed on the fly).
 * 
 */
public class EnwikiDocMaker extends LineDocMaker {
  ThreadLocal docData = new ThreadLocal();

  public EnwikiDocMaker(String fileName, StreamProvider stream) {
    super(fileName, stream);
  }

  private static final Map<String,Integer> ELEMENTS = new HashMap<String,Integer>();
  static final int TITLE = 0;
  static final int DATE = TITLE + 1;
  static final int BODY = DATE + 1;
  static final int ID = BODY + 1;
  static final int LENGTH = ID + 1;
  // LENGTH is used as the size of the tuple, so whatever constants we need that
  // should not be part of the tuple, we should define them after LENGTH.
  static final int PAGE = LENGTH + 1;

  static final String[] months = { "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
      "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };

  static {
    ELEMENTS.put("page", new Integer(PAGE));
    ELEMENTS.put("text", new Integer(BODY));
    ELEMENTS.put("timestamp", new Integer(DATE));
    ELEMENTS.put("title", new Integer(TITLE));
    ELEMENTS.put("id", new Integer(ID));
  }

  /**
   * Returns the type of the element if defined, otherwise returns -1. This
   * method is useful in startElement and endElement, by not needing to compare
   * the element qualified name over and over.
   */
  private final static int getElementType(String elem) {
    Integer val = (Integer) ELEMENTS.get(elem);
    return val == null ? -1 : val.intValue();
  }

  protected boolean keepImages = true;

  class Parser extends DefaultHandler implements Runnable {
    Thread t;
    boolean threadDone;

    public void run() {

      try {
        XMLReader reader = XMLReaderFactory.createXMLReader();
        reader.setContentHandler(this);
        reader.setErrorHandler(this);
        while (true) {
          final InputStream localFileIS = fileIS;
          try {
            InputSource is = new InputSource(localFileIS);
            reader.parse(is);
          } catch (IOException ioe) {
            ioe.printStackTrace();
            synchronized (EnwikiDocMaker.this) {
              if (localFileIS != fileIS) {
                // fileIS was closed on us, so, just fall
                // through

              } else
                // Exception is real
                throw ioe;
            }
          }
          synchronized (this) {
            if (!forever) {

              nmde = new NoMoreDataException();
              notify();
              return;
            } else if (localFileIS == fileIS) {
              // If file is not already re-opened then
              // re-open it now
              try {
                open();
              } catch (CompressorException e) {
                e.printStackTrace();
              }
            }
          }
        }
      } catch (SAXException sae) {
        throw new RuntimeException(sae);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      } finally {
        synchronized (this) {
          threadDone = true;
          notify();
        }
      }
    }

    public void error(SAXParseException e) throws SAXException {

      e.printStackTrace();
    }

    String[] tuple;
    NoMoreDataException nmde;
    Object lock = "lock";

    String[] next() throws NoMoreDataException {

      if (t == null) {
        threadDone = false;
        t = new Thread(this);
        t.setDaemon(true);
        t.start();
      }

      String[] result;

      synchronized (this) {
        while (tuple == null && nmde == null && !threadDone) {
          try {
            wait();
          } catch (InterruptedException ie) {
          }
        }

        if (nmde != null) {
          // Set to null so we will re-start thread in case
          // we are re-used:
          t = null;

          nmde.printStackTrace();
          throw nmde;
        }
        if (t != null && threadDone) {
          // The thread has exited yet did not hit end of
          // data, so this means it hit an exception. We
          // throw NoMorDataException here to force
          // benchmark to stop the current alg:

          throw new NoMoreDataException();
        }
        result = tuple;
        tuple = null;
        notify();
      }

      return result;
    }

    StringBuilder contents = new StringBuilder();

    public void characters(char[] ch, int start, int length) {
      contents.append(ch, start, length);
    }

    String title;
    String body;
    String time;
    String id;

    public void startElement(String namespace, String simple, String qualified,
        Attributes attributes) {
      int elemType = getElementType(qualified);
      switch (elemType) {
        case PAGE:
          title = null;
          body = null;
          time = null;
          id = null;
          break;
        // intentional fall-through.
        case BODY:
        case DATE:
        case TITLE:
        case ID:
          contents.setLength(0);
          break;
        default:
          // this element should be discarded.
      }
    }

    String time(String original) {
      StringBuilder buffer = new StringBuilder();

      buffer.append(original.substring(8, 10));
      buffer.append('-');
      buffer
          .append(months[Integer.valueOf(original.substring(5, 7)).intValue() - 1]);
      buffer.append('-');
      buffer.append(original.substring(0, 4));
      buffer.append(' ');
      buffer.append(original.substring(11, 19));
      buffer.append(".000");

      return buffer.toString();
    }

    public void create(String title, String time, String body, String id) {
      String[] t = new String[LENGTH];
      t[TITLE] = title.replace('\t', ' ');
      t[DATE] = time.replace('\t', ' ');
      t[BODY] = body.replaceAll("[\t\n]", " ");
      t[ID] = id;
      synchronized (this) {
        while (tuple != null) {
          try {
            wait();
          } catch (InterruptedException ie) {
          }
        }
        tuple = t;

        notify();
      }
    }

    public void endElement(String namespace, String simple, String qualified)
        throws SAXException {
      int elemType = getElementType(qualified);
      switch (elemType) {
        case PAGE:
          // the body must be null and we either are keeping image docs or the
          // title does not start with Image:
          if (body != null && (keepImages || !title.startsWith("Image:"))) {
            create(title, time, body, id);
          }
          break;
        case BODY:
          body = contents.toString();
          // workaround that startswith doesn't have an ignore case option, get
          // at least 20 chars.
          String startsWith = body
              .substring(0, Math.min(10, contents.length())).toLowerCase();
          if (startsWith.startsWith("#redirect")) {
            body = null;
          }
          break;
        case DATE:
          time = time(contents.toString());
          break;
        case TITLE:
          title = contents.toString();
          break;
        case ID:
          id = contents.toString();
          break;
        default:
          // this element should be discarded.
      }
    }
  }

  Parser parser = new Parser();

  private DocData getDocData() {
    DocData ds = (DocData) docData.get();
    if (ds == null) {
      ds = new DocData();
      docData.set(ds);
    }
    return ds;
  }

  public synchronized DocData makeDocument() throws Exception {
    String[] tuple = parser.next();
    DocData doc = getDocData();
    doc.title = tuple[TITLE];
    doc.date = tuple[DATE];
    doc.body = tuple[BODY];
    doc.id = new UID().toString();

    return doc;
  }

}
