package org.apache.hadoop.hive.mastiffFlexibleEncoding.orc;
/**
adapted from ORC
@author wangmeng
 */


/**
 * An interface for recording positions in a stream.
 */
interface PositionRecorder {
  void addPosition(long offset);
}