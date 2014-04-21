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
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;

import FlexibleEncoding.ORC.RunLengthIntegerReader;
import FlexibleEncoding.Parquet.Binary;
import FlexibleEncoding.Parquet.Utils;
import cn.ac.ncic.mastiff.Chunk;
import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.io.MultiChunk;
import cn.ac.ncic.mastiff.io.coding.Compression.Algorithm;
import cn.ac.ncic.mastiff.utils.Bytes;

public class DeltaBinaryPackingStringReader implements Decoder {
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
  public  DeltaBinaryPackingStringReader(int sortedCol, int valueLen_, Algorithm algorithm ) {
    //System.out.println("81  MVDEcoder  gou zao  han shu ");
    // System.out.println("82   sortCol   "+sortedCol+"   valuenLen  "+valueLen );
    valueLen = valueLen_;
    compressAlgo = algorithm;

    mvChunk = new MultiChunk(sortedCol, true, true, valueLen_);
  }

  @Override
  public ValPair begin() throws IOException {
    //  System.out.println("89     begin()    ensureDecompressed()");
    //ensureDecompressed();
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
    // ensureDecompressed();
    //    System.out.println("106   end()    ensureDecompressed()");
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
    // System.out.println("151  nextChunk()   ensureDecompressed() ");
    //  ensureDecompressed();
    System.out.println("152   page  size "+page.length);
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
    //    System.out.println("166   getChunkByPosition   ensureDecompressed() ");
    //ensureDecompressed();
    System.out.println("167  page  size "+page.length);
    shadowChunk.setBuffer(page, offset +  3 * Bytes.SIZEOF_INT,
        offset + indexOffset, numPairs, startPos);

    return shadowChunk;
  }

  @Override
  public void reset() {
    curIdx = 1;
  }

  @Override
  public void reset(byte[] buffer, int offset, int length) throws IOException {
    //   LOG.info("180  MV decoder nextPage  ");
    // System.out.println("180  MV decoder nextPage  ");
    this.offset = offset;
    compressedSize = length;
    //   System.out.println("182  buffer  "+buffer.length+"  offset  "+offset+"  length  "+length);
    bb = ByteBuffer.wrap(buffer, offset, length);
    decompressedSize = bb.getInt();
    numPairs = bb.getInt();
    startPos = bb.getInt();
    //    System.out.println("188  decompressedSize  "+ decompressedSize+"  numPairs  "+numPairs+"  startPos  "+startPos);
    // System.out.println("Decompress a compressed page size " + compressedSize + " into a page size " + decompressedSize);

    curIdx = 0;
    indexOffset = valueLen == -1 ? decompressedSize - numPairs * Bytes.SIZEOF_INT : -1;

    if (compressAlgo == null||Algorithm.NONE==compressAlgo){
      //   inBuf.reset(buffer, offset + 3 * Bytes.SIZEOF_INT, compressedSize - 3 * Bytes.SIZEOF_INT);
      inBuf.reset(buffer, offset, length);
      page = ensureDecompressed() ;
      System.out.println("201   page  size  "+page.length+" ensureDecompressed()  "+ensureDecompressed().length);
    }
    else{
      decompressedSize=bb.getInt();
      inBuf.reset(buffer, offset + 4 * Bytes.SIZEOF_INT, length - 4 * Bytes.SIZEOF_INT);
      ensureDecompress() ;
      
      page =  CompressensureDecompressed();
    }
  }

  public  void ensureDecompress() throws IOException {
    org.apache.hadoop.io.compress.Decompressor decompressor = this.compressAlgo.getDecompressor();
    InputStream is = this.compressAlgo.createDecompressionStream(inBuf, decompressor, 0);
    //   System.out.println("238   "+inBuf.getLength());
    ByteBuffer buf = ByteBuffer.allocate(decompressedSize);
    // ByteBuffer buf = ByteBuffer.allocate(is.available());
    //  System.out.println("241  "+decompressedSize);
    IOUtils.readFully(is, buf.array(),0, buf.capacity());
    is.close(); 
    this.compressAlgo.returnDecompressor(decompressor);
    inBuf.reset(buf.array(), offset, buf.capacity());
  }
  public byte[]  CompressensureDecompressed() throws IOException {
//    FlexibleEncoding.ORC.DynamicByteArray  dynamicBuffer = new FlexibleEncoding.ORC.DynamicByteArray();
//    dynamicBuffer.add(inBuf.getData(), 0, inBuf.getLength());
//    //   System.out.println("232   "+dynamicBuffer.size());
//    ByteBuffer byteBuf = ByteBuffer.allocate(dynamicBuffer.size());
//    //  System.out.println("56  "+inBuf.getInt());
//    dynamicBuffer.setByteBuffer(byteBuf, 0, dynamicBuffer.size());
    System.out.println("280    inBuf.length   "+inBuf.getLength());
    FlexibleEncoding.Parquet.DeltaByteArrayReader reader = new FlexibleEncoding.Parquet.DeltaByteArrayReader();

    DataOutputBuffer   transfer=new DataOutputBuffer() ;
 //   transfer.write(inBuf.getData(), 12, inBuf.getLength()-12);
    transfer.write(inBuf.getData(), 0, inBuf.getLength());
    byte [] data =transfer.getData() ;
    System.out.println("286   byte [] data  "+  data.length+"  numPairs  "+numPairs);
    inBuf.close();
    Binary[] bin = new Utils().readData(reader, data, numPairs);

    System.out.println("2998   Binary[] bin   "+     bin.length);
    // bb = ByteBuffer.wrap(page, 0, page.length);
    //  int  count=0 ;
    DataOutputBuffer   decoding = new DataOutputBuffer();
    DataOutputBuffer   offset = new DataOutputBuffer();
    decoding.writeInt(decompressedSize);
    decoding.writeInt(numPairs);
    decoding.writeInt(startPos);
    int dataoffset=12;
    // int dataoffset=0 ;
    String  str ;
    for(int i=0; i < numPairs; i++) {
      str =bin[i].toStringUsingUTF8() ;
    
     // dataoffset += str.length()+2 ;
      decoding.writeUTF(str);
      if(i<5){
        System.out.println("304  bin[i]  "+str+"  decoding    "+ decoding.size());
      }
      dataoffset =decoding.size()  ;
    //  decoding.writeBytes(str);
      offset.writeInt(dataoffset);

      //      if(i<14){
      //        System.out.println("///////////////////////tmp=   "+tmp);
      //      }
    }
    
    System.out.println("315  offset.size() "+offset.size()+"  decoding.szie   "+decoding.size());
    System.out.println("316  dataoffet   "+dataoffset);
    //  System.out.println("number  of Pairs =  "+ceshi);
    decoding.write(offset.getData(), 0,offset.size());
    inBuf.close();
    offset.close();
   

    System.out.println("316   decoding   "+decoding.size()+"   "+decoding.getLength()+" decoding.getData()   "+decoding.getData().length);
    return decoding.getData();
  }



  //  byte [] data = writer.getBytes().toByteArray();
  //  Binary[] bin = Utils.readData(reader, data, values.length);
  @Override
  public byte[]  ensureDecompressed() throws IOException {
    System.out.println("280    inBuf.length   "+inBuf.getLength());
    FlexibleEncoding.Parquet.DeltaByteArrayReader reader = new FlexibleEncoding.Parquet.DeltaByteArrayReader();

    DataOutputBuffer   transfer=new DataOutputBuffer() ;
    transfer.write(inBuf.getData(), 12, inBuf.getLength()-12);
    byte [] data =transfer.getData() ;
    System.out.println("286   byte [] data  "+  data.length+"  numPairs  "+numPairs);
    inBuf.close();
    Binary[] bin = new Utils().readData(reader, data, numPairs);

    System.out.println("2998   Binary[] bin   "+     bin.length);
    // bb = ByteBuffer.wrap(page, 0, page.length);
    //  int  count=0 ;
    DataOutputBuffer   decoding = new DataOutputBuffer();
    DataOutputBuffer   offset = new DataOutputBuffer();
    decoding.writeInt(decompressedSize);
    decoding.writeInt(numPairs);
    decoding.writeInt(startPos);
    int dataoffset=12;
    // int dataoffset=0 ;
    String  str ;
    for(int i=0; i < numPairs; i++) {
      str =bin[i].toStringUsingUTF8() ;
    
     // dataoffset += str.length()+2 ;
      decoding.writeUTF(str);
      if(i<5){
        System.out.println("304  bin[i]  "+str+"  decoding    "+ decoding.size());
      }
      dataoffset =decoding.size()  ;
    //  decoding.writeBytes(str);
      offset.writeInt(dataoffset);

      //      if(i<14){
      //        System.out.println("///////////////////////tmp=   "+tmp);
      //      }
    }
    
    System.out.println("315  offset.size() "+offset.size()+"  decoding.szie   "+decoding.size());
    System.out.println("316  dataoffet   "+dataoffset);
    //  System.out.println("number  of Pairs =  "+ceshi);
    decoding.write(offset.getData(), 0,offset.size());
    inBuf.close();
    offset.close();
   

    System.out.println("316   decoding   "+decoding.size()+decoding.getLength()+" decoding.getData()   "+decoding.getData().length);
    return decoding.getData();
  }

  @Override
  public boolean skipToPos(int pos) {
    if (pos < startPos || pos >= startPos + numPairs)
      return false;

    return true;
  }

}
