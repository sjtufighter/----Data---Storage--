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

import FlexibleEncoding.Parquet.BytesInput;
import FlexibleEncoding.Parquet.CapacityByteArrayOutputStream;
import FlexibleEncoding.Parquet.ColumnDescriptor;
import FlexibleEncoding.Parquet.Dictionary;
import FlexibleEncoding.Parquet.DictionaryPage;
import FlexibleEncoding.Parquet.DictionaryValuesReader;
import FlexibleEncoding.Parquet.Encoding;
import FlexibleEncoding.Parquet.PrimitiveType;
import cn.ac.ncic.mastiff.Chunk;
import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.io.MultiChunk;
import cn.ac.ncic.mastiff.io.coding.Compression.Algorithm;
import cn.ac.ncic.mastiff.utils.Bytes;

public class DictionaryBitPackingRLEIntReader implements Decoder {

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
  public   DictionaryBitPackingRLEIntReader(int sortedCol, int valueLen_,Algorithm algorithm) {
    valueLen = valueLen_;
    compressAlgo = algorithm;
    mvChunk = new MultiChunk(sortedCol, true, true, valueLen_);
  }

  @Override
  public ValPair begin() throws IOException {
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
    //ensureDecompressed();
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
    this.offset = offset;
    compressedSize = length;
    bb = ByteBuffer.wrap(buffer, offset, length);
    decompressedSize = bb.getInt();
    numPairs = bb.getInt();
    startPos = bb.getInt();
    curIdx = 0;
    indexOffset = valueLen == -1 ? decompressedSize - numPairs * Bytes.SIZEOF_INT : -1;
    if (compressAlgo == null||Algorithm.NONE==compressAlgo){
      inBuf.reset(buffer, offset, length);
      page = ensureDecompressed() ;
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
    ByteBuffer buf = ByteBuffer.allocate(decompressedSize);
    IOUtils.readFully(is, buf.array(),0, buf.capacity());
    is.close(); 
    this.compressAlgo.returnDecompressor(decompressor);
    inBuf.reset(buf.array(), offset, buf.capacity());
  }
  public byte[]  CompressensureDecompressed() throws IOException {
    FlexibleEncoding.ORC.DynamicByteArray  dynamicBuffer = new FlexibleEncoding.ORC.DynamicByteArray();
    dynamicBuffer.add(inBuf.getData(),0, inBuf.getLength());
    ByteBuffer byteBuf = ByteBuffer.allocate(dynamicBuffer.size());
    dynamicBuffer.setByteBuffer(byteBuf, 0, dynamicBuffer.size());
    byteBuf.flip();
    DataInputBuffer  dib = new DataInputBuffer();
    dib.reset(byteBuf.array(),0, byteBuf.array().length);
    int dictionarySize=dib.readInt();
    int OnlydictionarySize=dib.readInt();
    dib.reset(byteBuf.array(),8, dictionarySize-4);
    byte[]  dictionaryBuffer=dib.getData() ;
    dib.reset(byteBuf.array(),4+dictionarySize, (byteBuf.array().length-dictionarySize-4));
    byte[]  dictionaryId=dib.getData() ;
    dib.close();
    DictionaryValuesReader cr = initDicReader(OnlydictionarySize,dictionaryBuffer,PrimitiveType.PrimitiveTypeName.INT32);
    cr.initFromPage(numPairs, dictionaryId, 0);
    DataOutputBuffer   decoding = new DataOutputBuffer();
    decoding.writeInt(decompressedSize);
    decoding.writeInt(numPairs);
    decoding.writeInt(startPos);
    for(int i=0; i < numPairs; i++) {
      int   tmp=cr.readInteger();
      decoding.writeInt(tmp);
    }
    byteBuf.clear();

    inBuf.close();
    return decoding.getData();

  }
  private DictionaryValuesReader initDicReader(int DictionarySize ,byte[] bytes, PrimitiveType.PrimitiveTypeName type)
      throws IOException {
    CapacityByteArrayOutputStream  cbs=new CapacityByteArrayOutputStream(bytes.length);
    cbs.write(bytes, 0, bytes.length);
    BytesInput  bytesInput =new   BytesInput.CapacityBAOSBytesInput(cbs) ;
    DictionaryPage dictionaryPage =new  DictionaryPage( bytesInput,DictionarySize,Encoding.PLAIN_DICTIONARY);
    final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"foo"}, type, 0, 0);
    Encoding    encoding=Encoding.PLAIN_DICTIONARY ;
    final Dictionary dictionary =  encoding.initDictionary(descriptor, dictionaryPage);    
    final DictionaryValuesReader cr = new DictionaryValuesReader(dictionary);
    return cr;
  }

  @Override
  public byte[]  ensureDecompressed() throws IOException {
    FlexibleEncoding.ORC.DynamicByteArray  dynamicBuffer = new FlexibleEncoding.ORC.DynamicByteArray();
    dynamicBuffer.add(inBuf.getData(), 12, inBuf.getLength()-12);
    ByteBuffer byteBuf = ByteBuffer.allocate(dynamicBuffer.size());
    dynamicBuffer.setByteBuffer(byteBuf, 0, dynamicBuffer.size());
    byteBuf.flip();
    DataInputBuffer  dib = new DataInputBuffer();
    dib.reset(byteBuf.array(),0, byteBuf.array().length);
    int dictionarySize=dib.readInt();
    int OnlydictionarySize=dib.readInt();
    dib.reset(byteBuf.array(),8, dictionarySize-4);
    byte[]  dictionaryBuffer=dib.getData() ;
    dib.reset(byteBuf.array(),4+dictionarySize, (byteBuf.array().length-dictionarySize-4));
    byte[]  dictionaryId=dib.getData() ;
    dib.close();
    DictionaryValuesReader cr = initDicReader(OnlydictionarySize,dictionaryBuffer,PrimitiveType.PrimitiveTypeName.INT32);
    cr.initFromPage(numPairs, dictionaryId, 0);
    DataOutputBuffer   decoding = new DataOutputBuffer();
    decoding.writeInt(decompressedSize);
    decoding.writeInt(numPairs);
    decoding.writeInt(startPos);
    for(int i=0; i < numPairs; i++) {
      int   tmp=cr.readInteger();
      decoding.writeInt(tmp);
    }
    byteBuf.clear();
    inBuf.close();
    return decoding.getData();
  }

  @Override
  public boolean skipToPos(int pos) {
    if (pos < startPos || pos >= startPos + numPairs)
      return false;

    return true;
  }

}