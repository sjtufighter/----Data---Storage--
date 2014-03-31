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

import cn.ac.ncic.mastiff.ValPair;

/**
 * <p>
 * Encoder is a common interface used to encode values with 
 * different encoding algorithms, such as RLE.
 * </p>
 */
public abstract class  Encoder {
  /**
   * Coding Type
   */
  
  public  ByteBuffer bb;
  public  byte[] page;

  public int pageCapacity;
  public int valueLen;

  public  int dataOffset;
  public  int numPairs = 0;
  public int startPos;
  public static enum CodingType { 
    /** 
     * Encode multi values into a buffer
     */
    MV, 
    /** 
     * Encode multi same values into a RLETriple
     */
    RunLengthEncodingInt,
    RunLengthEncodingByte,
    RunLengthEncodingLong,
    DeltaBinaryBitPackingZigZarInt,
    DeltaBinaryArrayZigZarByte,
    DeltaBinaryBitPackingZigZarLong,
    DictionaryBitPackingRLEInt,
    DictionaryBitPackingRLEByte,
    DictionaryBitPackingRLELong,
  }
  
  /**
   * Layout Type
   */
  public static enum Type { 
    /** the value is encoded as fixed-length cluster */
    FIXEDLEN, 
    /** the value is encodes as var-length cluster */
    VARLEN 
  }
  
  /** 
   * Reset the encoder to encoding other values
   */
  public abstract void reset();
  
  /**
   * Get the encoded page buffer.
   * @return the reference to the encoded data buffer.
   * @throws IOException
   */
  public abstract byte[] getPage() throws IOException;
  
  /**
   * Get the length of the encoded page buffer length.
   * <br> Note: this method should be called after <i>getPage()</i>
   * @return the length of the encoded page buffer.
   */
  public abstract int getPageLen();
  
  /**
   * Append a value pair into the encoding buffer.
   * @param pair value pair
   * @return true if successful, otherwise false
   * @throws IOException
   */
  public abstract boolean append(ValPair pair) throws IOException;
  public abstract boolean appendPage(ValPair pair)  throws IOException;
}
