package org.apache.hadoop.hive.mastiff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.MTableDesc;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import cn.ac.ncic.mastiff.etl.ETLUtils;
import cn.ac.ncic.mastiff.hive.serde.lazy.ClusterAccessor.DataType;
import cn.ac.ncic.mastiff.mapred.MastiffMapReduce.TableDesc;

public class SegmentFileOutputFormat extends
FileOutputFormat<WritableComparable, BytesRefArrayWritable> implements
HiveOutputFormat<WritableComparable, BytesRefArrayWritable> {
  //private SegmentFile.Writer outWriter;

  @Override
  public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
    // TODO Auto-generated method stub

  }

  /**
   * set number of columns into the given configuration.
   *
   * @param conf
   *          configuration instance which need to set the column number
   * @param columnNum
   *          column number for SegmentFile's Writer
   *
   */
  @Override
  public org.apache.hadoop.mapred.RecordWriter getRecordWriter(FileSystem arg0, JobConf arg1,
      String arg2, Progressable arg3)
          throws IOException {
    throw new RuntimeException("Error: Hive should not invoke this method.");
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jc, Path finalOutPath, Class valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    String tblName = (String) tableProperties.get(MastiffHandlerUtil.CF_TABLE_NAME);
    try {
      MastiffHandlerUtil.setCFMeta(jc, tblName);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //  System.out.println(" //////////////////getRecordWriter  finalOutPath.getName()"+ finalOutPath.getName());
    MTableDesc tblDesc = MastiffHandlerUtil.getMTableDesc(tableProperties);
    MastiffHandlerUtil.getCFTypes(tblDesc);
    List<List<DataType>> clusterTypes = new ArrayList<List<DataType>>();
    SerializeUtil.jc = jc;

    SerializeUtil.fs = finalOutPath.getFileSystem(jc);
    SerializeUtil.tableProperties = tableProperties;
    // SerializeUtil.outputPath = finalOutPath;
    SerializeUtil.desc = new TableDesc();
    SerializeUtil.desc.columnsMapping = tblDesc.columnsMapping;
    SerializeUtil.desc.clusterCodingTypes = tblDesc.clusterCodingTypes;
    SerializeUtil.desc.clusterAlgos = tblDesc.clusterAlgos;
    DataType[][] tableSchema = new DataType[1][tblDesc.columnTypes.length];
    // List<DataType> cols = new ArrayList<DataType>();
    for (int i = 0; i < tblDesc.clusterTypes.length; i++) {
      clusterTypes.add(new ArrayList());
      for (int j = 0; j < tblDesc.clusterTypes[i].length; j++) {
        switch (((PrimitiveTypeInfo) tblDesc.clusterTypes[i][j]).getPrimitiveCategory()) {
        case BOOLEAN:
          clusterTypes.get(i).add(DataType.BOOLEAN);
          tableSchema[0][SerializeUtil.desc.columnsMapping[i][j]] = DataType.BOOLEAN;
          break;
        case BYTE:
          clusterTypes.get(i).add(DataType.BYTE);
          tableSchema[0][SerializeUtil.desc.columnsMapping[i][j]] = DataType.BYTE;
          break;
        case SHORT:
          clusterTypes.get(i).add(DataType.SHORT);
          tableSchema[0][SerializeUtil.desc.columnsMapping[i][j]] = DataType.SHORT;
          break;
        case INT:
          clusterTypes.get(i).add(DataType.INT);
          tableSchema[0][SerializeUtil.desc.columnsMapping[i][j]] = DataType.INT;
          break;
        case LONG:
          clusterTypes.get(i).add(DataType.LONG);
          tableSchema[0][SerializeUtil.desc.columnsMapping[i][j]] = DataType.LONG;
          break;
        case FLOAT:
          clusterTypes.get(i).add(DataType.FLOAT);
          tableSchema[0][SerializeUtil.desc.columnsMapping[i][j]] = DataType.FLOAT;
          break;
        case DOUBLE:
          clusterTypes.get(i).add(DataType.DOUBLE);
          tableSchema[0][SerializeUtil.desc.columnsMapping[i][j]] = DataType.DOUBLE;
          break;
        case STRING:
          clusterTypes.get(i).add(DataType.STRING);
          tableSchema[0][SerializeUtil.desc.columnsMapping[i][j]] = DataType.STRING;
          break;
        case TIMESTAMP:
          clusterTypes.get(i).add(DataType.DATE);
          tableSchema[0][SerializeUtil.desc.columnsMapping[i][j]] = DataType.DATE;
          break;
        default:
          try {
            throw new Exception("not supported type");
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }

    for (int i = 0; i < clusterTypes.size(); i++) {
      for (int j = 0; j < clusterTypes.get(i).size(); j++) {
        if (clusterTypes.get(i).get(j) == DataType.DATE) {
          clusterTypes.get(i).set(j, DataType.LONG);
        }
      }
    }
    SerializeUtil.desc.clusterTypes = clusterTypes;
    SerializeUtil.desc.tableSchema = tableSchema;
    String filename = ETLUtils.getOutputName(ETLUtils.getPartion(jc));
    Path outputPath = new Path(finalOutPath, filename);
    Path tmpoutputPath = new Path("/tmp/hive-mastiff/");
    Path tmpPath = new Path(tmpoutputPath, filename);
    //    FileSystem fs =finalOutPath.getFileSystem(jc);
    //    int buffsize =fs.getConf().getInt("io.file.buffer.size", 4096);
    //   short block= fs.getDefaultReplication();
    //   long blocksize=fs.getDefaultBlockSize();
    //    fs.create(outputPath,false, buffsize, block, blocksize) ;
    //   System.out.println(" ///////////////////// outputPath=    "+ outputPath.getName());
    if(finalOutPath.getFileSystem(jc).exists(tmpPath)){
      finalOutPath.getFileSystem(jc).delete(tmpPath, true);
    }
    final SegmentFile.Writer  outWriter = new SegmentFile.Writer(finalOutPath.getFileSystem(jc), tmpPath,outputPath,tmpoutputPath,
        clusterTypes);
    // SerializeUtil.writer = outWriter;
    //outWriter.
    // System.out.println(" /////////////////////SegmentFile.Writer  outWriter)=    "+outWriter);
    return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {
      @Override
      public void write(Writable r) throws IOException {
        //  System.out.println(" ////////////////////////outWriter.append(r)=    "+r);
        outWriter.append(r);
      }

      @Override
      public void close(boolean abort) throws IOException {
        outWriter.close();
      }
    };
  }
}
