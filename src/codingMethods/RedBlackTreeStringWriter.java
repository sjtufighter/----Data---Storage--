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

import org.apache.hadoop.io.DataOutputBuffer;

import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.io.coding.Compression.Algorithm;
import cn.ac.ncic.mastiff.utils.Bytes;

public class RedBlackTreeStringWriter extends Encoder {
  Algorithm compressAlgo;
  org.apache.hadoop.io.compress.Compressor compressor;
  DataOutputBuffer outputBuffer = new DataOutputBuffer();
  public  RedBlackTreeStringWriter(int pageLen_, int valueLen_ , int startPos_, Algorithm algorithm) {
    pageCapacity = pageLen_;
    startPos = startPos_;
    this.compressAlgo = algorithm;
    valueLen = valueLen_;
    reset();
  }

  @Override
  public boolean append(ValPair pair) throws IOException {
    if (bytesLeft < pair.length + Bytes.SIZEOF_INT)
      return false;
    System.arraycopy(pair.data, 0, page, dataOffset, pair.length);
    numPairs++;
    dataOffset += pair.length;
    // index it offset
    index.writeInt(dataOffset);
    bytesLeft -= pair.length;
    bytesLeft -= Bytes.SIZEOF_INT;
    return true;
  }


  @Override
  public byte[] getPage() throws IOException {
    if (dataOffset == 3 * Bytes.SIZEOF_INT) // no data appended
      return null;
    if (compressAlgo == null) {
      bb = ByteBuffer.wrap(page, 0, page.length);
      bb.putInt(pageCapacity - bytesLeft);
      //   bb.putInt(page.length);
      bb.putInt(numPairs);
      bb.putInt(startPos);
   //   System.out.println("91    bytesLeft  "+bytesLeft+"  dataOffset "+ dataOffset+"  page.length  "+page.length+ " numPairs  "+numPairs+" pageCapacity "+pageCapacity+"  startPos  "+startPos);
      return page;
    } else {
 //     System.out.println("93     pageCapacity  "+pageCapacity+" bytesLeft  "+bytesLeft+" numPairs  "+numPairs+" startPos "+startPos);
      outputBuffer.reset();
      outputBuffer.writeInt(pageCapacity - bytesLeft);
      outputBuffer.writeInt(numPairs);
      outputBuffer.writeInt(startPos);
      compressor = compressAlgo.getCompressor();
      OutputStream os =
          this.compressAlgo.createCompressionStream(outputBuffer, compressor, 0);
      os.write(page, 3 * Bytes.SIZEOF_INT, pageCapacity - bytesLeft - 3 * Bytes.SIZEOF_INT);
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
      //   return pageCapacity - bytesLeft;
      return page.length;
    else
      return outputBuffer.getLength();
  }

  @Override
  public void reset() {
    startPos += numPairs;  
    numPairs = 0;
    dataOffset = 3 * Bytes.SIZEOF_INT;
    bytesLeft = pageCapacity - dataOffset;
    index.reset();
  }

  @Override
  public boolean appendPage(ValPair pair) {
    page =new byte[pair.length+12];
    System.arraycopy(pair.data, 0, page, 12, pair.length);
    return true;
  }


}
