package org.apache.hadoop.hive.mastiff;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import cn.ac.ncic.mastiff.ValPair;

/**
 * RowWritable is used to pass each record's data and
 * query's metadata from {@link SegmentFileRecordReader} to {@link MastiffSerDe} <br/>
 * 
 * Since we could not get selected column ids from conf in serde, the metadata
 * of selected columns should be the workaround to get that from RecordReader.
 * In order to pass the query's metadata, two kinds of RowWritable object are involved
 * <ul>
 * <li>The first record passed from recordReader, we fill the RowWritable with valid cf ids and
 * valid columns along with the real data</li>
 * <li>Other records except for first, only its data was set</li> </li>
 */
public class RowWritable implements Writable {

  private byte isFirst; // first 1, others 0
  private List<ValPair> vps;
  private List<Integer> validCfs;
  private List<Integer> validColumns;

  public RowWritable() {
  }

  /**
   * Set the data to be transported
   * 
   * @param isFirst
   *          1 for first record, 0 for the others
   * @param curVps
   *          Current line's data
   * @param validCfs
   *          Query's selected cf
   * @param validColumns
   *          Query's selected columns
   */
  public void set(byte isFirst, List<ValPair> curVps, List<Integer> validCfs,
      List<Integer> validColumns) {
    this.isFirst = isFirst;
    vps = curVps;
    if (validCfs != null) {
      this.validCfs = validCfs;
    }
    if (validColumns != null) {
      this.validColumns = validColumns;
    }
  }


  /**
   * First Record : <code>|1|num_cfs|num_columns|valid_cfs|valid_columns|real_data|</code> <br/>
   * Other Records: <code>|0|num_cfs|real_data|</code>
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(isFirst);
    out.writeInt(vps.size());
    if (isFirst == 1) {
      out.writeInt(validColumns.size());
      for (int i = 0; i < validCfs.size(); i++) {
        out.writeInt(validCfs.get(i));
      }
      for (int j = 0; j < validColumns.size(); j++) {
        out.writeInt(validColumns.get(j));
      }
    }
    for (int k = 0; k < vps.size(); k++) {
      vps.get(k).write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    isFirst = in.readByte();
    int vpcount = in.readInt();
    if (vps == null) {
      vps = new ArrayList<ValPair>();
    } else {
      vps.clear();
    }
    if (isFirst == 1) { // first record
      int clmcount = in.readInt();
      validCfs = new ArrayList<Integer>();
      validColumns = new ArrayList<Integer>();
      for (int i = 0; i < vpcount; i++) {
        validCfs.add(in.readInt());
      }
      for (int j = 0; j < clmcount; j++) {
        validColumns.add(in.readInt());
      }
    }
    for (int k = 0; k < vpcount; k++) {
      ValPair curVp = new ValPair();
      curVp.readFields(in);
      vps.add(curVp);
    }
  }

  public List<ValPair> getVps() {
    return vps;
  }

  public List<Integer> getValidCfs() {
    return validCfs;
  }

  public byte getIsFirst() {
    return isFirst;
  }

  public List<Integer> getValidColumns() {
    return validColumns;
  }

}
