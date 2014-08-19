package org.apache.hadoop.hive.mastiff;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

import cn.ac.ncic.mastiff.hive.serde.lazy.ClusterAccessor.DataType;
import cn.ac.ncic.mastiff.hive.serde.lazy.Row;

public class RowMap implements WritableComparable {
  public int numClusters;
  public Row[] row;// BytesWritable
  public RowMap(int numClusters, DataType[][] clusterTypes, List<List<DataType>> clusterSchema) {
    super();
    this.numClusters = numClusters;
    row = new Row[numClusters];
    for (int m = 0; m < numClusters; m++) {
      row[m] = new Row(clusterSchema.get(m));
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    numClusters = in.readInt();
    for (int i = 0; i < numClusters; i++) {
      row[i].readFields(in);
    }
  }
  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    out.writeInt(numClusters);
    for (int i = 0; i < numClusters; i++) {
      row[i].write(out);
    }
  }

  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return 0;
  }

}
