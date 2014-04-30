package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;

/*
 * adapted  from Parquet*
 */



public interface IntPackerFactory {

  IntPacker newIntPacker(int width);

}
