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
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;

import cn.ac.ncic.mastiff.Chunk;
import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.io.MultiChunk;
import cn.ac.ncic.mastiff.io.coding.Compression.Algorithm;
import cn.ac.ncic.mastiff.utils.Bytes;

/**
 * <p>
 * Decode the <i>MultiChunk</i>s from a encode page.
 * A encoded page just produce one <i>MultiChunk</i>
 * </p>
 * 
 * @see MultiChunk
 */
public class MVDecoder implements Decoder {
  static final Log LOG = LogFactory.getLog(MVDecoder.class);
  ValPair pair = new ValPair();
  int valueLen;

  MultiChunk mvChunk;
  MultiChunk shadowChunk = null;

  /** Compressed data */
  byte[] page = null;
  int offset;
  int compressedSize;
  int decompressedSize;

  /** statistic of a page */
  int numPairs;
  int startPos;
  ByteBuffer bb;

  /** a index area if the cluster is var-length */
  int indexOffset;

  // used for iteration
  int curIdx = 1;

  Algorithm compressAlgo;
  DataInputBuffer inBuf = new DataInputBuffer();

  /**
   * MVDecoder 
   * @param sortedCol which column to sorted
   * @param valueLen_ the length of the value
   * @param algorithm which compression algorithm used to compress the data
   */
  public MVDecoder(int sortedCol, int valueLen_, Algorithm algorithm) {
    System.out.println("81  MVDEcoder  gou zao  han shu ");
    System.out.println("82   sortCol   "+sortedCol+"   valuenLen  "+valueLen );
    valueLen = valueLen_;
    compressAlgo = algorithm;

    mvChunk = new MultiChunk(sortedCol, true, true, valueLen_);
  }

  @Override
  public ValPair begin() throws IOException {
    System.out.println("89     begin()    ensureDecompressed()");
    ensureDecompress();
    pair.data = page;
    pair.offset = offset + 3 * Bytes.SIZEOF_INT;
    pair.length = valueLen == -1 ?  
        Bytes.toInt(page, offset + indexOffset) - 3 * Bytes.SIZEOF_INT : valueLen;
        pair.pos = startPos;
        return pair;
  }

  @Override
  public int beginPos() {
    return startPos;
  }

  @Override
  public ValPair end() throws IOException {
    ensureDecompress();
    System.out.println("106   end()    ensureDecompressed()");
    if (numPairs == 1) 
      return begin();
    else {
      pair.data = page;
      pair.pos = startPos + numPairs - 1;
      if (valueLen == -1) {
        int lastPairOffset = Bytes.toInt(page, offset + indexOffset + (numPairs - 2) * Bytes.SIZEOF_INT);
        pair.offset = offset + lastPairOffset;
        pair.length = indexOffset - lastPairOffset;
      } else {
        pair.offset = offset + 3 * Bytes.SIZEOF_INT + (numPairs - 1) * valueLen;
        pair.length = valueLen;
      }
      return pair;
    }
  }

  @Override
  public int endPos() {
    return startPos + numPairs - 1;
  }

  @Override
  public byte[] getBuffer() {
    return page;
  }

  @Override
  public int getBufferLen() {
    return decompressedSize;
  }

  @Override
  public int getNumPairs() {
    return numPairs;
  }

  @Override
  public boolean hashNextChunk() {
    return curIdx < 1;
  }

  @Override
  public Chunk nextChunk() throws IOException {
    if (curIdx >= 1) return null;
    System.out.println("151  nextChunk()   ensureDecompressed() ");
    ensureDecompress();
    mvChunk.setBuffer(page, offset +  3 * Bytes.SIZEOF_INT,
        offset + indexOffset, numPairs, startPos);
    curIdx++;
    return mvChunk;
  }

  @Override
  public Chunk getChunkByPosition(int position) throws IOException {
    if (position < startPos || position >= startPos + numPairs)
      return null;

    if (shadowChunk == null)
      shadowChunk = new MultiChunk(0, true, true, valueLen);
    System.out.println("166   getChunkByPosition   ensureDecompressed() ");
    ensureDecompress();
    System.out.println("172  page.length  "+page.length+"  offset "+offset+" offset + indexOffset   "+(offset + indexOffset)+"  numPairs "+numPairs+" startPos  "+startPos);
    shadowChunk.setBuffer(page, offset +  3 * Bytes.SIZEOF_INT,
        offset + indexOffset, numPairs, startPos);

    return shadowChunk;
  }

  @Override
  public void reset() {
    curIdx = 1;
  }

  @Override
  public void reset(byte[] buffer, int offset, int length) {
    LOG.info("180  MV decoder nextPage  ");
    System.out.println("180  MV decoder nextPage  ");
    this.offset = offset;
    compressedSize = length;

    bb = ByteBuffer.wrap(buffer, offset, compressedSize);
    decompressedSize = bb.getInt();
    numPairs = bb.getInt();
    startPos = bb.getInt();
   
    // System.out.println("Decompress a compressed page size " + compressedSize + " into a page size " + decompressedSize);

    curIdx = 0;
    indexOffset = valueLen == -1 ? decompressedSize - numPairs * Bytes.SIZEOF_INT : -1;

    if (compressAlgo == null)
      page = buffer;
    else {
      inBuf.reset(buffer, offset + 3 * Bytes.SIZEOF_INT, compressedSize - 3 * Bytes.SIZEOF_INT);
    
      page = null;
    }
  }

 public  void ensureDecompress() throws IOException {
    if (compressAlgo != null && page == null) {
      org.apache.hadoop.io.compress.Decompressor decompressor = this.compressAlgo.getDecompressor();
      InputStream is = this.compressAlgo.createDecompressionStream(inBuf, decompressor, 0);
      ByteBuffer buf = ByteBuffer.allocate(decompressedSize);
      IOUtils.readFully(is, buf.array(), 3 * Bytes.SIZEOF_INT, buf.capacity() - 3 * Bytes.SIZEOF_INT);
      is.close(); 
      this.compressAlgo.returnDecompressor(decompressor);
      page = buf.array();
      System.out.println("213   ensureDecompressed()   "+page.length);
    }
  }

  @Override
  public boolean skipToPos(int pos) {
    if (pos < startPos || pos >= startPos + numPairs)
      return false;

    return true;
  }

  @Override
  public byte[] ensureDecompressed() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
