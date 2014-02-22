package org.apache.hadoop.hive.mastiff;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

/**
 * A split for a segment used by a Map task.
 *
 * @see InputSplit
 */
public class SegmentFileSplit extends FileSplit {

  Path path;

  long segmentOffset;
  long segmentPMSOffset;
  long segmentLength = 0;
  String[] hosts;
  long length;
  int segId;
  String tableName;


  public SegmentFileSplit() {
    super((Path) null, 0, 0, (String[]) null);
  }

  public SegmentFileSplit(int segId, Path file, long segOffsets, long segPMSOffsets,
      long segLengths, long length, String[] hosts, String tableName) {
    super((Path) null, 0, 0, (String[]) null);
    this.segId = segId;
    path = file;
    segmentOffset = segOffsets;
    segmentPMSOffset = segPMSOffsets;
    segmentLength = segLengths;
    this.hosts = hosts;
    this.length = length;
    this.tableName = tableName;
  }

  @Override
  public Path getPath() {
    return path;
  }

  public long getSegOffset() {
    return segmentOffset;
  }

  public long getSegPmsOffset() {
    return segmentPMSOffset;
  }

  public long getSegLength() {
    return segmentLength;
  }

  public int getSegId() {
    return segId;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Table ").append(" :");
    sb.append(path.toString()).append("@seg").append(segId);
    sb.append('[');
    if (segmentLength != 0) {
      sb.append('(').append(segmentOffset).append(',')
          .append(segmentPMSOffset).append(',')
          .append(segmentLength).append(')');
    }
    sb.append(']');
    return sb.toString();
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException {
    if (hosts == null) {
      return new String[] {};
    } else {
      return hosts;
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    segId = in.readInt();
    path = new Path(Text.readString(in));
    length = in.readLong();
    segmentOffset = in.readLong();
    segmentPMSOffset = in.readLong();
    segmentLength = in.readLong();
    tableName = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(segId);
    Text.writeString(out, path.toString());
    out.writeLong(length);
    out.writeLong(segmentOffset);
    out.writeLong(segmentPMSOffset);
    out.writeLong(segmentLength);
    out.writeUTF(tableName);
  }

}
