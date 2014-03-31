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

import cn.ac.ncic.mastiff.Chunk;
import cn.ac.ncic.mastiff.ValPair;

/**
 * A common <i>Decoder</i> interface to decode values from encoded buffer.
 */
public interface Decoder {

  /**
   * Reset decoding buffer.
   * @param buffer the encoded data buffer
   * @param offset the offset of the encoded buffer in the bytes array
   * @param length the length of the encoded data buffer
   * @throws IOException
   */
  public void reset(byte buffer[], int offset, int length) 
      throws IOException;

  public void reset();

  /**
   * @return the reference to the data buffer
   */
  public byte[] getBuffer();

  /**
   * @return the length of the data buffer
   */
  public int getBufferLen();

  /**
   * Is there any encoded chunk left in the encoded buffer.
   * @return true if yes, false if no
   */
  public boolean hashNextChunk();

  /**
   * Get the next encoded chunk
   * @return the encoded chunk
   * @throws IOException
   */
  public Chunk nextChunk() throws IOException;

  /**
   * Get a chunk by position
   * @return
   * @throws IOException
   */
  public Chunk getChunkByPosition(int position) throws IOException;

  /**
   * Skip to the target position
   * @param pos
   * @return true if success, otherwise false
   */
  public boolean skipToPos(int pos);

  /**
   * The first value pair in the encoded data buffer
   * @return value pair
   * @throws IOException
   */
  public ValPair begin() throws IOException;
  /**
   * The last value pair in the encoded data buffer
   * @return value pair
   * @throws IOException
   */
  public ValPair end() throws IOException;

  /**
   * @return the begin position 
   * @throws IOException
   */
  public int beginPos() throws IOException;
  /**
   * @return the end position
   * @throws IOException
   */
  public int endPos() throws IOException;

  /**
   * @return number of the value pairs encoded in the buffer
   */
  public int getNumPairs();
  public  byte[]  ensureDecompressed() throws IOException ;
}
