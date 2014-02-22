package org.apache.hadoop.hive.mastiff;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDeStatsStruct;
import org.apache.hadoop.hive.serde2.lazy.LazyNonPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;

/**
 *
 * LazyMastiffRowBase. Adapted from {@link LazyStruct} <br/>
 * Change the {@link #fields} type
 * from LazyObject[] into Object[] to store java primitive types directly
 *
 */
public abstract class LazyMastiffRowBase extends LazyNonPrimitive<LazyMastiffRowObjectInspector>
    implements
    SerDeStatsStruct {

  private static Log LOG = LogFactory.getLog(LazyMastiffRowBase.class.getName());

  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed;

  /**
   * Size of serialized data
   */
  long serializedSize;

  /**
   * The start positions of struct fields. Only valid when the data is parsed.
   * Note that startPosition[arrayLength] = begin + length + 1; that makes sure
   * we can use the same formula to compute the length of each element of the
   * array.
   */
  int[] startPosition;

  /**
   * The fields of the struct.
   */
  Object[] fields;
  /**
   * Whether init() has been called on the field or not.
   */
  boolean[] fieldInited;

  /**
   * Construct a LazyStruct object with the ObjectInspector.
   */
  public LazyMastiffRowBase(LazyMastiffRowObjectInspector oi) {
    super(oi);
  }

  @Override
  public Object getObject() {
    return this;
  }

  protected boolean getParsed() {
    return parsed;
  }

  protected void setParsed(boolean parsed) {
    this.parsed = parsed;
  }

  protected Object[] getFields() {
    return fields;
  }

  protected void setFields(Object[] fields) {
    this.fields = fields;
  }

  protected boolean[] getFieldInited() {
    return fieldInited;
  }

  protected void setFieldInited(boolean[] fieldInited) {
    this.fieldInited = fieldInited;
  }

  @Override
  public long getRawDataSerializedSize() {
    return serializedSize;
  }
}
