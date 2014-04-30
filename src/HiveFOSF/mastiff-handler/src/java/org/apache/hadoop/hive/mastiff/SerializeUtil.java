package org.apache.hadoop.hive.mastiff;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

public class SerializeUtil {
  public static final Log LOG = LogFactory.getLog(SerializeUtil.class.getName());
  public static cn.ac.ncic.mastiff.mapred.MastiffMapReduce.TableDesc desc = null;
  public static JobConf jc;
  public static Properties tableProperties;
  public static FileSystem fs;
  public static Reporter reporter;
  public static class PageId implements WritableComparable<PageId> {
    String segmentId;
    int clusterIdx;
    int pageIdx;
    int hashcode = 0;

    public PageId() {
    }
    public PageId duplicate() {
      PageId sid = new PageId();
      sid.segmentId = new String(segmentId);
      sid.clusterIdx = clusterIdx;
      sid.pageIdx = pageIdx;
      sid.hashcode = hashcode;
      return sid;
    }

    public void copy(PageId sid) {
      segmentId = sid.segmentId;
      clusterIdx = sid.clusterIdx;
      pageIdx = sid.pageIdx;
      hashcode = sid.hashcode;
    }

    public boolean isInSameCluster(PageId sid) {
      return segmentId.equals(sid.segmentId) && clusterIdx == sid.clusterIdx;
    }

    public boolean isInSameSegment(PageId sid) {
      return segmentId.equals(sid.segmentId);
    }

    public void setSegmentId(String segmentId) {
      this.segmentId = segmentId;
      hashcode = segmentId.hashCode();
    }

    public String getPos() {
      return segmentId;
    }

    public void setClusterId(int cluster) {
      clusterIdx = cluster;
    }

    public int getClusterId() {
      return clusterIdx;
    }

    public void setPageId(int pageId) {
      pageIdx = pageId;
    }

    public int getPageId() {
      return pageIdx;
    }

    @Override
    public int compareTo(PageId other) {
      int cmp = segmentId.compareTo(other.segmentId);
      if (cmp == 0) {
        cmp = clusterIdx - other.clusterIdx;
        if (cmp == 0) {
          return pageIdx - other.pageIdx;
        } else {
          return cmp;
        }
      }
      return cmp;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof PageId)) {
        return false;
      }
      return compareTo((PageId) obj) == 0;
    }

    @Override
    public int hashCode() {
      return hashcode;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      clusterIdx = in.readInt();
      pageIdx = in.readInt();
      segmentId = Text.readString(in);
      hashcode = segmentId.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(clusterIdx);
      out.writeInt(pageIdx);
      Text.writeString(out, segmentId);

    }

  }
}
