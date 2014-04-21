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

public class RedBlackTreeString implements Decoder {

  @Override
  public void reset(byte[] buffer, int offset, int length) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub

  }

  @Override
  public byte[] getBuffer() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getBufferLen() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean hashNextChunk() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Chunk nextChunk() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Chunk getChunkByPosition(int position) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean skipToPos(int pos) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ValPair begin() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ValPair end() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int beginPos() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int endPos() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getNumPairs() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public byte[] ensureDecompressed() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
