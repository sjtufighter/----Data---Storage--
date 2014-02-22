package org.apache.hadoop.hive.mastiff;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

import cn.ac.ncic.mastiff.hive.serde.lazy.ClusterAccessor.DataType;
import cn.ac.ncic.mastiff.hive.serde.lazy.Row;

public class RowMap  implements WritableComparable{
  /*RowMap is used for analyis a source row  as the segmentfile  organization
   * @author wangmeng
   */

  // public int numClusters = SerializeUtil.desc.clusterTypes.size();
  private  int numClusters ;
  public final    Row[]  row ;//BytesWritable
  // public  final int[][]  columnsMapping =SerializeUtil.desc.columnsMapping ;

  //private final   DataType[][] originalClusterTypes =SerializeUtil.desc.tableSchema ;
  //  private     DataType[][] clusterTypes = null ;
  //  private    List<List<DataType>> clusterSchema =null ;
  public RowMap( int numClusters,DataType[][] clusterTypes , List<List<DataType>> clusterSchema) {
    super();
    //  for (int i = 0; i < originalClusterTypes. length ; i++) {
    //    clusterTypes[i] = new DataType[originalClusterTypes[i]. length];
    //    for (int j = 0; j < clusterTypes[i]. length; j++) {
    //      if (originalClusterTypes[i][j] == DataType. DATE) {
    //        clusterTypes[i][j] = DataType. LONG ;
    //      } else {
    //        clusterTypes[i][ j ] = originalClusterTypes [i][j];
    //      }
    //    }
    //  }
    //    for (int i = 0; i < SerializeUtil.desc.clusterTypes.size(); i++) {
    //      for (int j = 0; j < SerializeUtil.desc.clusterTypes.get(i).size(); j++) {
    //        if (SerializeUtil.desc.clusterTypes.get(i).get(j) == DataType.DATE) {
    //          SerializeUtil.desc.clusterTypes.get(i).set(j, DataType.LONG);
    //        }
    //      }
    //    }
    //    clusterTypes = ETLUtils.getSchema(SerializeUtil.desc.clusterTypes);
    //    clusterSchema = ETLUtils.getSchema(clusterTypes);
    this.numClusters=numClusters ;
    row= new Row[numClusters];
    for (int m=0;m<numClusters;m++){
      row[m] = new Row(clusterSchema.get(m));
    }
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    numClusters=in.readInt();
    // row= new Row[numClusters];
    for (int i=0;i<numClusters;i++){
      row[i].readFields(in);
    }
  }
  //  for (int i = 0; i < numClusters; i++) {
  //    int j = 0;
  //    for (int col : columnsMapping[i]) {
  //      rows[i].setValue(j, originalRow.getValue(col));
  //      j++;
  //    }
  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    out.writeInt(numClusters);
    for (int i=0;i<numClusters;i++){
      row[i].write(out);
    }
  }

  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return 0;
  }

}