package org.apache.hadoop.hive.mastiff;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
 * <ul><li>The first record passed from recordReader, we fill the RowWritable with valid
 * cf ids and valid columns along with the real data</li>
 *<li>Other records except for first, only its data was set</li>
 *</li>
 */
/*used to read the segmentFile Row
 *
 *@author wangmeng shenyijie
 *
 */
public class RowWritable implements Writable{

  private byte isFirst; //first 1, others 0
  private ValPair[] vps;
  private Integer[] validCfs;
  private Integer[] validColumns;

  public RowWritable() {
  }

  /**
   * Set the data to be transported
   * @param isFirst 1 for first record, 0 for the others
   * @param curVps Current line's data
   * @param validCfs Query's selected cf
   * @param validColumns Query's selected columns
   */
  public void set(byte isFirst, List<ValPair> curVps, List<Integer> validCfs, List<Integer> validColumns) {
    this.isFirst = isFirst;
    vps = curVps.toArray(new ValPair[0]);
    if(validCfs != null) {
      this.validCfs = validCfs.toArray(new Integer[0]);
    }
    if(validColumns != null) {
      this.validColumns = validColumns.toArray(new Integer[0]);
    }
  }


  /**
   * First Record : <code>|1|num_cfs|num_columns|valid_cfs|valid_columns|real_data|</code> <br/>
   * Other Records: <code>|0|num_cfs|real_data|</code>
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(isFirst);
    out.writeInt(vps.length);
    if(isFirst == 1) {
      out.writeInt(validColumns.length);
      for(int i = 0; i < vps.length; i++) {
        out.writeInt(validCfs[i]);
      }
      for(int j = 0; j < validColumns.length; j++) {
        out.writeInt(validColumns[j]);
      }
    }
    for(int k = 0; k < vps.length; k++) {
      vps[k].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    isFirst = in.readByte();
    int vpcount = in.readInt();
    vps = new ValPair[vpcount];
    if(isFirst == 1) { //first record
      int clmcount = in.readInt();
      validCfs = new Integer[vpcount];
      validColumns = new Integer[clmcount];
      for(int i = 0; i < vpcount; i++) {
        validCfs[i] = in.readInt();
      }
      for(int j = 0; j < clmcount; j++) {
        validColumns[j] = in.readInt();
      }
    }
    for(int k = 0; k < vpcount; k++) {
      vps[k] = new ValPair();
      vps[k].readFields(in);
    }
  }

  public ValPair[] getVps() {
    return vps;
  }

  public Integer[] getValidCfs() {
    return validCfs;
  }

  public byte getIsFirst() {
    return isFirst;
  }

  public Integer[] getValidColumns() {
    return validColumns;
  }

}
