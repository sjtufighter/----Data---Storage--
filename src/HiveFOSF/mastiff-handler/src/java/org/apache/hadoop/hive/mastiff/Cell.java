package org.apache.hadoop.hive.mastiff;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.utils.Bytes;

/**
 * Cell represents the underling structure of a given column family (cf)
 * <p>
 * In deserialization procedure, each cf is associate with a Cell object and used in the following
 * order:
 * <ol>
 * <li>
 * init the object with cf's column types, this result in the exactly position (offset & length) of
 * each fixed length field and index position of var length fields</li>
 * <li>
 * for each ValPair corresponds to this cf, calculate the exactly position of var length fields</li>
 * </ol>
 *
 * Adapted from {@link cn.ac.ncic.mastiff.hive.serde.lazy.ClusterAccessor}
 */
public class Cell {

  /**
   *
   * ClmLocation is used to store field's position in a given {@link cn.ac.ncic.mastiff.ValPair}
   *
   */
  static class ClmLocation {
    public int offset;
    public int length;
    public int indirectBeginOffset;
    public int indirectEndOffset;
  }

  List<TypeInfo> cols;
  List<ClmLocation> locations;

  List<Integer> aligned8;
  List<Integer> aligned4;
  List<Integer> aligned2;
  List<Integer> unaligned_fixed;
  List<Integer> unaligned_var;

  // the offset of the area that stores all the offsets of variable fields
  int firstIndirectOffset;
  int lastIndirectOffset;
  // the offset of the area that stores all the variable data
  int firstVarOffset;

  // used to calculate the offsets of each fileds
  int curLen;

  // the aligned bytes
  int alignedBytes;

  public Cell() {
  }

  /**
   * Get exactly position of fixed length fields
   * and index position of var length fields
   *
   * @param colTypes
   *          Type infos correspond to the column family
   */
  public void init(List<TypeInfo> colTypes) {
    cols = colTypes;

    aligned8 = new ArrayList<Integer>();
    aligned4 = new ArrayList<Integer>();
    aligned2 = new ArrayList<Integer>();
    unaligned_fixed = new ArrayList<Integer>(); // Byte & Boolean
    unaligned_var = new ArrayList<Integer>(); // String
    locations = new ArrayList<ClmLocation>();

    curLen = 0;

    for (int i = 0; i < cols.size(); i++) {
      int datalen = getDataLen(cols.get(i));
      boolean isFixedType = datalen > 0;
      if (!isFixedType) { // varlen columns
        unaligned_var.add(i);
      } else {
        switch (datalen) {
        case 4:
          aligned4.add(i);
          break;
        case 8:
          aligned8.add(i);
          break;
        case 2:
          aligned2.add(i);
          break;
        case 1:
          unaligned_fixed.add(i);
          break;
        }
      }
      ClmLocation curLoc = new ClmLocation();
      curLoc.length = datalen;
      locations.add(curLoc);
    }
    // Calculate the aligned bytes
    alignedBytes = 1;

    if (aligned8.size() > 0) {
      alignedBytes = 8;
    } else if (aligned4.size() > 0) {
      alignedBytes = 4;
    } else if (aligned2.size() > 0) {
      alignedBytes = 2;
    }

    initFixedLenObjects(aligned8);
    initFixedLenObjects(aligned4);
    initFixedLenObjects(aligned2);

    firstIndirectOffset = unaligned_var.size() == 0 ? -1 : curLen;
    int lastIndirectOffset = -1;
    for (int col : unaligned_var) {
      ClmLocation curLoc = locations.get(col);
      curLoc.indirectBeginOffset = lastIndirectOffset;
      lastIndirectOffset = curLoc.indirectEndOffset = curLen;
      curLen += Bytes.SIZEOF_SHORT;
      // System.err.println("Data Indirect : " + ls.indirectBeginOffset + ", " +
      // ls.indirectEndOffset);
    }
    lastIndirectOffset = unaligned_var.size() == 0 ? -1 : curLen - Bytes.SIZEOF_SHORT;

    initFixedLenObjects(unaligned_fixed);

    if (unaligned_var.size() != 0) {
      locations.get(unaligned_var.get(0)).offset = curLen;
      firstVarOffset = curLen;
      alignedBytes = 2;
    } else {
      firstVarOffset = -1;
    }
  }

  /**
   * Get each field's location (offset & length) in the current vp</br>
   * Fixed length fields' location have already been calculated in init,
   * so we just calculate var length fields'
   *
   * @param vp
   *          Current vp to be processed
   */
  public void getFieldLocation(ValPair vp) {
    for (Integer colId : unaligned_var) {
      ClmLocation curLocation = locations.get(colId);
      int beginOffset, endOffset;
      if (curLocation.indirectBeginOffset == -1) { // first var length
        beginOffset = curLocation.offset;
      } else {
        beginOffset = Bytes.toShort(vp.data, vp.offset + curLocation.indirectBeginOffset);
      }
      endOffset = Bytes.toShort(vp.data, vp.offset + curLocation.indirectEndOffset);
      curLocation.offset = beginOffset;
      curLocation.length = endOffset - beginOffset;
    }
  }

  public boolean isVarLen() {
    return firstVarOffset != -1;
  }

  public int getFixedLen() {
    return firstVarOffset != -1 ? -1 : curLen;
  }

  /**
   * Get type's length in SegmentFile
   *
   * @param type
   * @return type's length in bytes
   */
  private static int getDataLen(TypeInfo type) {
    String typeName = type.getTypeName();
    if (typeName.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
      return Bytes.SIZEOF_BOOLEAN;
    } else if (typeName.equals(serdeConstants.BINARY_TYPE_NAME)) {
      return Bytes.SIZEOF_BYTE;
    } else if (typeName.equals(serdeConstants.SMALLINT_TYPE_NAME)) {
      return Bytes.SIZEOF_SHORT;
    } else if (typeName.equals(serdeConstants.INT_TYPE_NAME)) {
      return Bytes.SIZEOF_INT;
    } else if (typeName.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      return Bytes.SIZEOF_LONG;
    } else if (typeName.equals(serdeConstants.FLOAT_TYPE_NAME)) {
      return Bytes.SIZEOF_FLOAT;
    } else if (typeName.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      return Bytes.SIZEOF_DOUBLE;
    } else if (typeName.equals(serdeConstants.STRING_TYPE_NAME)) {
      return 0;
    } else if (typeName.equals(serdeConstants.DATE_TYPE_NAME)) {
      return Bytes.SIZEOF_LONG;
    } else if (typeName.equals(serdeConstants.TIMESTAMP_TYPE_NAME)) {
      return Bytes.SIZEOF_LONG;
    } else if (typeName.equals(serdeConstants.TINYINT_TYPE_NAME)) {
      return Bytes.SIZEOF_BYTE;
    }
    else {
      return -1;
    }
  }

  public List<ClmLocation> getLocations() {
    return locations;
  }

  private void initFixedLenObjects(List<Integer> cols) {
    for (int col : cols) {
      locations.get(col).offset = curLen;
      curLen += locations.get(col).length;
      // System.err.println("Cur Length : " + curLen);
    }
  }

}
