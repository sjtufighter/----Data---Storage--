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
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import cn.ac.ncic.mastiff.Chunk;
import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.io.RLEChunk;
import cn.ac.ncic.mastiff.io.RLETriple;
import cn.ac.ncic.mastiff.utils.Bytes;

/**
 * <p>
 * Decode the <i>RLEChunk</i>s from a encode page.
 * An encoded page will produce multi <i>RLEChunk</i>s.
 * </p>
 * 
 * @see RLEChunk
 */
public class GuoSiJieRLEDecoder implements Decoder {

  ValPair pair = new ValPair();
  int valueLen;
  
  /** RLE related structures */
  RLEChunk rleBlock;
  RLETriple rleTriple;
  ValPair pairInTriple;
  
  RLEChunk shadowChunk;
  RLETriple shadowTriple;
  ValPair shadowVP;
  
  /** Encoded page data */
  byte[] page = null;
  int offset;
  int length;
  
  /** statistics of page data */
  int numPairs;
  int startPos;
  
  /** used for iteration */
  int curIdx = -1;
  int curTripleOffset;
  int curTripleLength;
  int tripleOffsetInPage;
  int curStartPos = -1;
  int curNumReps = 0;
  
  /** used for var-length cluster */
  ByteBuffer bb;
  IntBuffer index = null;
  int indexOffset;
  
  public GuoSiJieRLEDecoder(int sortedCol, int valueLen_) {
   System.out.println("77  RLE Decoder ");
   System.out.println("sortedCol=   "+sortedCol+"   valueLen_   "+valueLen_);
    pairInTriple = new ValPair();
    rleTriple = new RLETriple(pairInTriple, 0, 0);
    rleBlock = new RLEChunk(rleTriple, sortedCol);
    
    shadowVP = new ValPair();
    shadowTriple = new RLETriple(shadowVP, 0, 0);
    shadowChunk = new RLEChunk(shadowTriple, sortedCol);
    
    valueLen = valueLen_;
  }
  
  @Override
  public ValPair begin() {
    System.out.println("92  begin ");
    pair.data = page;
    pair.offset = offset + 3 * Bytes.SIZEOF_INT;
    if (valueLen > 0) 
      pair.length = valueLen;
    else {
      pair.length = index.get(0) - 3 * Bytes.SIZEOF_INT - 2 * Bytes.SIZEOF_INT;
    }
    pair.pos = startPos;
    return pair;
  }

  @Override
  public int beginPos() {
    return startPos;
  }

  @Override
  public ValPair end() {
    System.out.println("111   end()");
    pair.data = page;
    if (valueLen > 0) {
      pair.offset = offset + 3 * Bytes.SIZEOF_INT + 
      (numPairs - 1) * (valueLen + 2 * Bytes.SIZEOF_INT);
      pair.length = valueLen;
    } else {
      int prevOffset = numPairs == 1 ? 3 * Bytes.SIZEOF_INT :
        index.get(numPairs-2);
      pair.length = index.get(numPairs-1) - prevOffset - 2 * Bytes.SIZEOF_INT;
      pair.offset = offset + prevOffset;
    }
    int startPosInLastTriple = Bytes.toInt(page, pair.offset + pair.length);
    int numRepsInLastTriple = Bytes.toInt(page, pair.offset + pair.length + Bytes.SIZEOF_INT);
    pair.pos = startPosInLastTriple + numRepsInLastTriple - 1;
    return pair;
  }

  @Override
  public int endPos() {
    return end().pos;
  }

  @Override
  public byte[] getBuffer() {
    return page;
  }

  @Override
  public int getBufferLen() {
    return length;
  }

  @Override
  public int getNumPairs() {
    return endPos() - beginPos() + 1;
  }

  @Override
  public boolean hashNextChunk() {
    if (curIdx < 0 || curIdx >= numPairs)
      return false;
    else return true;
  }

  @Override
  public Chunk nextChunk() throws IOException {
    System.out.println("158   nextChunk()");
    if (curIdx < 0 || curIdx >= numPairs) return null;
    
    curTripleLength = valueLen > 0 ? valueLen : 
      index.get(curIdx) - tripleOffsetInPage - 2 * Bytes.SIZEOF_INT;
    
    pairInTriple.data = page;
    pairInTriple.offset = curTripleOffset;
    pairInTriple.length = curTripleLength;
    curStartPos = pairInTriple.pos = Bytes.toInt(page, 
        curTripleOffset + curTripleLength, Bytes.SIZEOF_INT);
    curNumReps = Bytes.toInt(page, curTripleOffset + curTripleLength + Bytes.SIZEOF_INT, 
                             Bytes.SIZEOF_INT);
    rleTriple.setTriple(pairInTriple, pairInTriple.pos, curNumReps);
    
    curTripleOffset += curTripleLength + 2 * Bytes.SIZEOF_INT;
    tripleOffsetInPage += curTripleLength + 2 * Bytes.SIZEOF_INT;
    curIdx++;
    
    rleBlock.reset();
    return rleBlock;
  }

  @Override
  public Chunk getChunkByPosition(int position) throws IOException {

    System.out.println("184   getchunkByPosition");
    int begin = 0, end = numPairs - 1, mid, mid_pos, num_reps;
    int tripleOffset = -1, tripleLength = valueLen;
    // var length cluster
    while (begin <= end) {
      mid = (begin + end) / 2;
      if (valueLen < 0) {
        tripleOffset = mid == 0 ? 3 * Bytes.SIZEOF_INT : index.get(mid - 1);
        tripleLength = index.get(mid) - tripleOffset - 2 * Bytes.SIZEOF_INT;
      } else {
        tripleOffset = 3 * Bytes.SIZEOF_INT + mid * (valueLen + 2 * Bytes.SIZEOF_INT);
      }
      mid_pos = Bytes.toInt(page, offset + tripleOffset + tripleLength);
      num_reps = Bytes.toInt(page, offset + tripleOffset + tripleLength + Bytes.SIZEOF_INT);
      if (mid_pos <= position && position < mid_pos + num_reps) {
        shadowVP.data = page;
        shadowVP.offset = offset + tripleOffset;
        shadowVP.length = tripleLength;
        shadowTriple.setTriple(shadowVP, mid_pos, num_reps);
        shadowChunk.reset();
        return shadowChunk;
      } else if (position < mid_pos) {
        end = mid - 1;
      } else {
        begin = mid + 1;
      }
    }
    return null;
  }

  @Override
  public void reset() {
    curIdx = -1;
  }

  @Override
  public void reset(byte[] buffer, int offset, int length) throws IOException {
    System.out.println("214   RLEDecoder  reset ");
    this.page = buffer;
    this.offset = offset;
    this.length = length;
    
    
    numPairs = Bytes.toInt(page, offset + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
    
    curIdx = 0;
    curStartPos = -1;
    curTripleOffset = offset + 3 * Bytes.SIZEOF_INT;
    tripleOffsetInPage = 3 * Bytes.SIZEOF_INT;
    if (valueLen < 0) {
      indexOffset = this.length - numPairs * Bytes.SIZEOF_INT;
      // System.out.println("Index offset in Page is " + indexOffset);
      bb = ByteBuffer.wrap(page, offset + indexOffset, numPairs * Bytes.SIZEOF_INT);
      index = bb.asIntBuffer();
      
    }
    
    int tmpTripleLength = valueLen > 0 ? valueLen : 
      index.get(0) - tripleOffsetInPage - 2 * Bytes.SIZEOF_INT;
    startPos = Bytes.toInt(page, curTripleOffset + tmpTripleLength, Bytes.SIZEOF_INT);
    
    // System.err.println("NumTriples in Page is " + numPairs + ", start from pos " + startPos);
    
  }

  @Override
  public boolean skipToPos(int pos) {
    System.out.println("251   skipToPosition");
    // System.err.println("[RLEDecompressor]skipToPos : skip to pos " + pos + ", curStartPos is " + curStartPos);
    if (curStartPos == -1) {
      curTripleLength = valueLen > 0 ? valueLen : 
        index.get(curIdx) - tripleOffsetInPage - 2 * Bytes.SIZEOF_INT;
      curStartPos = Bytes.toInt(page, curTripleOffset + curTripleLength, Bytes.SIZEOF_INT);
      curNumReps = Bytes.toInt(page, curTripleOffset + curTripleLength + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
    }
    while(curStartPos + curNumReps < pos && curIdx < numPairs) {
      curTripleOffset += (curTripleLength + 2 * Bytes.SIZEOF_INT);
      tripleOffsetInPage += (curTripleLength + 2 * Bytes.SIZEOF_INT);
      curTripleLength = valueLen > 0 ? valueLen : 
        index.get(curIdx) - tripleOffsetInPage - 2 * Bytes.SIZEOF_INT;
      
      curStartPos = Bytes.toInt(page, curTripleOffset + curTripleLength, Bytes.SIZEOF_INT);
      curNumReps = Bytes.toInt(page, curTripleOffset + curTripleLength + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
      curIdx++;
    }
    // System.err.println("[RLEDecompressor]skipToPos : now @ pos " + curStartPos + ", numReps " + curNumReps + ".");
    if (curIdx >= numPairs)
      return false;
    else
      return true;
  }

  @Override
  public byte[] ensureDecompressed() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
