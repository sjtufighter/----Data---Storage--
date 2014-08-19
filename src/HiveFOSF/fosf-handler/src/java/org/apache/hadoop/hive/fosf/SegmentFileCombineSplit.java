package org.apache.hadoop.hive.mastiff;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

public class SegmentFileCombineSplit extends FileSplit {

  private List<SegmentFileSplit> parts;
  private String[] hosts;

  // generated fields
  private long totLength = 0;
  private Path[] paths;
  private long[] segmentOffsets;
  private long[] segmentPMSOffsets;
  private long[] segmentLengths;

  private boolean genFieldsInited;

  SegmentFileCombineSplit() {
    super((Path) null, 0, 0, (String[]) null);
    genFieldsInited = false;
  }

  SegmentFileCombineSplit(List<SegmentFileSplit> parts, String[] hosts) {
    super((Path) null, 0, 0, (String[]) null);
    genFieldsInited = false;
    assert parts.size() > 0;
    this.parts = new ArrayList<SegmentFileSplit>();
    for (SegmentFileSplit sfs : parts) {
      this.parts.add(sfs);
    }
    this.hosts = hosts;

    initSplit();
  }

  private void initSplit() {
    totLength = 0;
    paths = new Path[parts.size()];
    segmentOffsets = new long[parts.size()];
    segmentPMSOffsets = new long[parts.size()];
    segmentLengths = new long[parts.size()];

    int i = 0;
    for (SegmentFileSplit sfs : parts) {
      paths[i] = sfs.path;
      segmentOffsets[i] = sfs.segmentOffset;
      segmentPMSOffsets[i] = sfs.segmentPMSOffset;
      segmentLengths[i] = sfs.segmentLength;
      totLength += sfs.segmentLength;
      i++;
    }
    genFieldsInited = true;
  }

  public List<SegmentFileSplit> getParts() {
    return parts;
  }

  @Override
  public long getLength() {
    if (!genFieldsInited) {
      initSplit();
    }
    return totLength;
  }

  @Override
  public Path getPath() {
    if (!genFieldsInited) {
      initSplit();
    }
    return getPath(0);
  }

  @Override
  public long getStart() {
    if (!genFieldsInited) {
      initSplit();
    }
    return segmentOffsets[0];
  }

  public Path getPath(int i) {
    if (!genFieldsInited) {
      initSplit();
    }
    if (paths != null) {
      return paths[i];
    }
    return new Path("/empty");
  }

  public Path[] getPaths() {
    if (!genFieldsInited) {
      initSplit();
    }
    return paths;
  }

  public String getTableName() {
    return parts.get(0).getTableName();
  }

  @Override
  public String[] getLocations() throws IOException {
    return hosts;
  }

  public long[] getSegmentOffsets() {
    if (!genFieldsInited) {
      initSplit();
    }
    return segmentOffsets;
  }

  public long[] getSegmentPMSOffsets() {
    if (!genFieldsInited) {
      initSplit();
    }
    return segmentPMSOffsets;
  }

  public long[] getSegmentLengths() {
    if (!genFieldsInited) {
      initSplit();
    }
    return segmentLengths;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < paths.length; i++) {
      if (i == 0) {
        sb.append("Paths:");
      }
      sb.append(paths[i].toUri().getPath() + ":" + parts.get(i).segmentOffset +
          "+" + parts.get(i).segmentLength);
      if (i < paths.length - 1) {
        sb.append(",");
      }
    }
    if (hosts != null) {
      String locs = "";
      StringBuffer locsb = new StringBuffer();
      for (int i = 0; i < hosts.length; i++) {
        locsb.append(hosts[i] + ":");
      }
      locs = locsb.toString();
      sb.append(" Locations:" + locs + "; ");
    }
    return sb.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int segNum = in.readInt();
    parts = new ArrayList<SegmentFileSplit>();
    for (int si = 0; si < segNum; si++) {
      SegmentFileSplit curSfs = new SegmentFileSplit();
      curSfs.readFields(in);
      parts.add(curSfs);
    }
    int hostNum = in.readInt();
    hosts = new String[hostNum];
    for (int hi = 0; hi < hostNum; hi++) {
      hosts[hi] = in.readUTF();
    }
    initSplit();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(parts.size());
    for (SegmentFileSplit sfs : parts) {
      sfs.write(out);
    }
    out.writeInt(hosts.length);
    for (int i = 0; i < hosts.length; i++) {
      out.writeUTF(hosts[i]);
    }
  }


}
