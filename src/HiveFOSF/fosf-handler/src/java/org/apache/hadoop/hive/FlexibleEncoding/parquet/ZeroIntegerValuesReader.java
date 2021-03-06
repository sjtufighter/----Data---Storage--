
/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;

import java.io.IOException;



/**
 * ColumnReader which does not read any actual data, but rather simply produces
 * an endless stream of constant values.
 * Mainly used to read definition levels when the only possible value is 0
 */
public class ZeroIntegerValuesReader extends ValuesReader {

  private int nextOffset;

  @Override
  public int readInteger() {
    return 0;
  }

  @Override
  public void initFromPage(int valueCount, byte[] in, int offset) throws IOException {
    this.nextOffset = offset;
  }

  @Override
  public int getNextOffset() {
    return nextOffset;
  }

  @Override
  public void skip() {
  }

}