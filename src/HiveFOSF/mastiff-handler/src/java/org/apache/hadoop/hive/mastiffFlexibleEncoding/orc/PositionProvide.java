package org.apache.hadoop.hive.mastiffFlexibleEncoding.orc;
/**
adapted from ORC
@author wangmeng
 */



/**
 * An interface used for seeking to a row index.
 */
interface PositionProvider {
  long getNext();
}