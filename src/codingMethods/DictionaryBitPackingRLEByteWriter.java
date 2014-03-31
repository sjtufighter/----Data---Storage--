/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ncic.mastiff.io.coding;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;

import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.io.coding.Compression.Algorithm;
import cn.ac.ncic.mastiff.utils.Bytes;

public class DictionaryBitPackingRLEByteWriter extends Encoder {


  static final Log LOG = LogFactory.getLog(FixedLenEncoder.class);
  /** 
   * Which compression algorithm used to compress a page of data 
   * 
   * @see Algorithm
   */
  Algorithm compressAlgo;
  /**
   * Used the compressor provided by hadoop to compress the data
   * 
   * @see org.apache.hadoop.io.compress.Compressor
   */
  org.apache.hadoop.io.compress.Compressor compressor;
  DataOutputBuffer outputBuffer = new DataOutputBuffer();

  /**
   * FixeLenEncoder Constructor
   * 
   * @param pageLen_
   *            the page capacity
   * @param valueLen_
   *            the length of the value
   * @param startPos_
   *            the start position
   * @param algorithm
   *            the compression algorithm
   * 
   * @see Algorithm
   */
  public  DictionaryBitPackingRLEByteWriter(int pageLen_, int valueLen_, int startPos_, Algorithm algorithm) {
    pageCapacity = pageLen_;
    valueLen = valueLen_;
    startPos = startPos_;
    compressAlgo = algorithm;

    page = new byte[pageCapacity];
    if (LOG.isDebugEnabled()) {
      LOG.debug("FixeLenCompressor : " + pageCapacity + ", " + valueLen + ", " + startPos);
    }
    reset();
  }

  @Override
  public void reset() {
    startPos += numPairs;  
    numPairs = 0;
    dataOffset = 3 * Bytes.SIZEOF_INT;
  }

  @Override
  public boolean append(ValPair pair) {
    if (dataOffset + valueLen > pageCapacity)
    {
      return false;
    }
    System.arraycopy(pair.data, 0, page, dataOffset, pair.length);
    numPairs++;
    dataOffset += valueLen;

    return true;
  }

  @Override
  public boolean appendPage(ValPair pair) {
    //    if (dataOffset + valueLen > pageCapacity)
    //      return false;
    //   LOG.info("121 ValPair  length "+pair.length);
    //   System.out.println("121 ValPair  length "+pair.length);

    page =new byte[pair.length+12];
    System.arraycopy(pair.data, 0, page, 12, pair.length);
    //    numPairs++;
    //    dataOffset += valueLen;

    return true;
  }

  @Override
  public byte[] getPage() throws IOException {
    if (dataOffset == 3 * Bytes.SIZEOF_INT) // no data appended 
      return null;
    /** We do not compressed the page of data */
    if (compressAlgo == null) {
      //  bb = ByteBuffer.wrap(page, 0, dataOffset);
      bb = ByteBuffer.wrap(page, 0, page.length);
      bb.putInt(dataOffset);
      bb.putInt(numPairs);
      bb.putInt(startPos);
      //   LOG.info("134  page .length "+page.length );
      //  System.out.println("154 dataoffset  "+dataOffset+" numPairs  "+numPairs+"  startPos  "+startPos);
      return page;
    } else { // compress a page using the specified <i>Algorithm</i>
      outputBuffer.reset();
      outputBuffer.writeInt(dataOffset);
      outputBuffer.writeInt(numPairs);
      outputBuffer.writeInt(startPos);
      outputBuffer.writeInt(page.length-12);
      compressor = compressAlgo.getCompressor();
      // create the compression stream
      OutputStream os =
          this.compressAlgo.createCompressionStream(outputBuffer, compressor, 0);
      os.write(page, 3 * Bytes.SIZEOF_INT, dataOffset - 3 * Bytes.SIZEOF_INT);
      os.flush();
      this.compressAlgo.returnCompressor(compressor);
      this.compressor = null;
      return outputBuffer.getData();
    }
  }

  @Override
  public int getPageLen() {
    if (dataOffset == 3 * Bytes.SIZEOF_INT) // no data appended
      return 0;

    if (compressAlgo == null)
      // return dataOffset;
      return   page.length;

    else
      return outputBuffer.getLength();
  }

}
