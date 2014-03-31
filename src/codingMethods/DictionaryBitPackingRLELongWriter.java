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

import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.io.coding.Compression.Algorithm;

public class DictionaryBitPackingRLELongWriter  extends  Encoder {

  public DictionaryBitPackingRLELongWriter(int pageCapacity_, int valueLen_,
      int startPos_, Algorithm algo_) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public byte[] getPage() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getPageLen() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean append(ValPair pair) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean appendPage(ValPair pair) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

}
