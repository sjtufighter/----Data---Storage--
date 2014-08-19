package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;

/*
 * adapted  from Parquet*
 */

abstract public class GroupConverter extends Converter {

  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public GroupConverter asGroupConverter() {
    return this;
  }

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of the field in this group
   * @return the corresponding converter
   */
  abstract public Converter getConverter(int fieldIndex);

  /** runtime calls  **/

  /** called at the beginning of the group managed by this converter */
  abstract public void start();

  /**
   * call at the end of the group
   */
  abstract public void end();

}