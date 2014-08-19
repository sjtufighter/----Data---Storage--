package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;
/*
 * adapt from  parquet
 *
 */

public interface BytePackerFactory {

  BytePacker newBytePacker(int width);

}
