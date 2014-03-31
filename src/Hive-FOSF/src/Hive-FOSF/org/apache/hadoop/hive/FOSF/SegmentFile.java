package org.apache.hadoop.hive.mastiff;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.mastiff.SerializeUtil.PageId;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.orc.OutStream;
//import FlexibleEncoding.ORC.RunLengthByteWriter;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.orc.RunLengthByteWriter;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.orc.RunLengthIntegerWriter;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.orc.TestInStream;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet.Binary;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet.BytesInput;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet.DeltaBinaryPackingValuesWriter;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet.DeltaByteArrayWriter;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet.OnlyDictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet.OnlyDictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import cn.ac.ncic.mastiff.PosChunk;
import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.etl.ETLUtils;
import cn.ac.ncic.mastiff.hive.serde.lazy.ClusterAccessor;
import cn.ac.ncic.mastiff.hive.serde.lazy.ClusterAccessor.DataType;
import cn.ac.ncic.mastiff.hive.serde.lazy.Row;
import cn.ac.ncic.mastiff.io.PosRLEChunk;
import cn.ac.ncic.mastiff.io.coding.Compression.Algorithm;
import cn.ac.ncic.mastiff.io.coding.EnDecode;
import cn.ac.ncic.mastiff.io.coding.Encoder;
import cn.ac.ncic.mastiff.io.coding.Encoder.CodingType;
import cn.ac.ncic.mastiff.io.coding.VarLenEncoder;
import cn.ac.ncic.mastiff.io.segmentfile.BlockCache;
import cn.ac.ncic.mastiff.io.segmentfile.LruBlockCache;
import cn.ac.ncic.mastiff.io.segmentfile.PageCache;
import cn.ac.ncic.mastiff.io.segmentfile.PageMeta;
import cn.ac.ncic.mastiff.io.segmentfile.PageMeta.ScanMode;
import cn.ac.ncic.mastiff.io.segmentfile.PageMetaList;
import cn.ac.ncic.mastiff.io.segmentfile.PageMetaSection;
import cn.ac.ncic.mastiff.io.segmentfile.SimplePageCache;
import cn.ac.ncic.mastiff.mapred.MastiffMapReduce;
import cn.ac.ncic.mastiff.operators.ExprDesc;
import cn.ac.ncic.mastiff.operators.Predicate;
import cn.ac.ncic.mastiff.utils.Bytes;
import cn.ac.ncic.mastiff.utils.Utils;

/**
 * File format for mastiff.
 *
 * Copied from {@link cn.ac.ncic.mastiff.io.segmentfile.SegmentFile}
 */
public class SegmentFile {
  static final Log LOG = LogFactory.getLog(SegmentFile.class);

  public static final String SEGFILE_CACHE_SIZE_KEY = "mastiff.pagecache.size";

  private static BlockCache globalPageCache = null;

  /**
   * SegmentFile Writer.
   */
  public static class Writer implements Closeable {

    public class TabConfig {
      private  PlainBinaryDictionaryValuesWriter[]   dictionaryBitPackingRLEZigZarByte=null;
      private  PlainIntegerDictionaryValuesWriter[]   dictionaryBitPackingRLEZigZarInt=null;
      private  TestInStream.OutputCollector[]  collect=null;
      private  DeltaBinaryPackingValuesWriter[]  deltaBianryBitPackingInt=null ;
      private DeltaByteArrayWriter[]  deltaByteArrayWriter=null ;
      private RunLengthIntegerWriter[]   runLengthInteger=null;
      private  RunLengthByteWriter[]   runLengthByte=null;
      private boolean[]  pageInit;
      private  int[] pagesizes ;
      private Algorithm[] algorithms ;
      public boolean isInit=false ,isFirst=true ;
      private  DataType[][] originalTableSchema;
      private  List<List<DataType>> originalSchema;
      private int numFields;
      private int[] cluster_pages;
      private List<List<DataType>> clusterSchema;
      private int numClusters;
      private ClusterAccessor[] accessors;
      private  ClusterAccessor[] backups;
      private Row[] rows, prevRows, maxs, mins, segMaxs, segMins;
      private Encoder[] compressors;
      private CodingType[] codings;
      private ValPair[] vps;
      private  ValPair[] backupVps;
      private  PageMeta[] pms;
      private  int[] pageIds;
      private  long  pageIdCount=0 ;
      private  long sgementSize = 0;
      private int[] startPoss, numReps;
      public  PageId segId;
      //   HashMap<Integer, List<BytesWritable>> segmentValue = new HashMap<Integer, List<BytesWritable>>();
      //     private final  ArrayList<List<BytesWritable>> clusterValue = new ArrayList<List<BytesWritable>>(SerializeUtil.desc.clusterTypes.size());
      private  ArrayList<List<BytesWritable>> clusterValue =null;
      private final  BytesWritable outValue = new BytesWritable();
      private final DataOutputBuffer out = new DataOutputBuffer();
      //   private final   long SegmentSize=536870912-SerializeUtil.desc.clusterTypes.size()*131072;
      private final   long SegmentSize=33554432*8-SerializeUtil.desc.clusterTypes.size()*131072*4;
      //     private final   long SegmentSize=1048576-SerializeUtil.desc.clusterTypes.size()*131072;
      private final  int[] tmpLength = new int[1];
      private int[][] columnsMapping;
      private final  DataInputBuffer in = new DataInputBuffer();

      public void configure(JobConf job, Properties tbl) throws IOException {
        int numClusters = SerializeUtil.desc.clusterTypes.size();
        if (numClusters != SerializeUtil.desc.clusterAlgos.length) {
          throw new RuntimeException("Please check the cluster algorithms, " +
              SerializeUtil.desc.clusterAlgos.length + " algorithms provided while there are " +
              numClusters + " clusters.");
        }
        if (numClusters != SerializeUtil.desc.clusterCodingTypes.length) {
          throw new RuntimeException("Please check the cluster coding types, " +
              SerializeUtil.desc.clusterAlgos.length + " coding types provided while there are " +
              numClusters + " clusters.");
        }
        numFields = SerializeUtil.desc.tableSchema[0].length;
        originalTableSchema = SerializeUtil.desc.tableSchema;
        DataType[][] storageSchema = new DataType[originalTableSchema.length][];
        for (int i = 0; i < originalTableSchema.length; i++) {
          storageSchema[i] = new DataType[originalTableSchema[i].length];
          for (int j = 0; j < storageSchema[i].length; j++) {
            if (originalTableSchema[i][j] == DataType.DATE) {
              storageSchema[i][j] = DataType.LONG;
            } else {
              storageSchema[i][j] = originalTableSchema[i][j];
            }
          }
        }


        runLengthInteger=new  RunLengthIntegerWriter[numClusters] ;
        runLengthByte=new  RunLengthByteWriter[numClusters] ;
        collect=new  TestInStream.OutputCollector[numClusters] ;
        deltaBianryBitPackingInt=new DeltaBinaryPackingValuesWriter[numClusters] ;
        deltaByteArrayWriter=new DeltaByteArrayWriter[numClusters] ;
        dictionaryBitPackingRLEZigZarInt=new  PlainIntegerDictionaryValuesWriter[numClusters] ;
        dictionaryBitPackingRLEZigZarByte=new  PlainBinaryDictionaryValuesWriter[numClusters] ;
        pageInit=new boolean[numClusters];


        for(int i=0 ;i<numClusters;i++){
          pageInit[i]=false;

        }
        // replace DataType.DATE to DataType.LONG
        for (int i = 0; i < SerializeUtil.desc.clusterTypes.size(); i++) {
          for (int j = 0; j < SerializeUtil.desc.clusterTypes.get(i).size(); j++) {
            if (SerializeUtil.desc.clusterTypes.get(i).get(j) == DataType.DATE) {
              SerializeUtil.desc.clusterTypes.get(i).set(j, DataType.LONG);
            }
          }
        }
        //    TestInStream.OutputCollector[]  collect=null;
        //   FixedLenEncoder   fixedEncoding=null;
        //  RunLengthIntegerWriter[]   rleEncoding=null;
        // int pagesize = MastiffMapReduce.getTablePageSize(job);
        int pagesize = 131072*4;
        cluster_pages = new int[numClusters];

        for (int i = 0; i < numClusters; i++) {

          //          if (SerializeUtil.desc.clusterCodingTypes[i] == CodingType.RLE) {
          //            cluster_pages[i] = pagesize;
          //          } else {
          cluster_pages[i] = pagesize ;
          //          cluster_pages[i] = SerializeUtil.desc.clusterAlgos[i] == null ?
          //              pagesize : SerializeUtil.desc.clusterAlgos[i].getScaleRatio() * pagesize;
          //  }
        }
        //        for (int b = 0; b < numClusters; b++) {
        //          //  switch (b){
        //          //     case 0:
        //          //  clusterValue.add(new ArrayList<BytesWritable>(90));
        //          clusterValue.add(new ArrayList<BytesWritable>(512));
        //          //  //       break ;
        //          //     //  case 1:
        //          //       //  clusterValue.add(new ArrayList<BytesWritable>(175));
        //          //           clusterValue.add(new ArrayList<BytesWritable>(40));
        //          //   //      break ;
        //          //  //     case 2:
        //          //    //     clusterValue.add(new ArrayList<BytesWritable>(90));
        //          //         clusterValue.add(new ArrayList<BytesWritable>(40));
        //          ////         break ;
        //          //       case 3:
        //          //       //  clusterValue.add(new ArrayList<BytesWritable>(680));
        //          //         clusterValue.add(new ArrayList<BytesWritable>(40));
        //          //         break ;
        //          //       case 4:
        //          //        clusterValue.add(new ArrayList<BytesWritable>(45));
        //          //         break ;
        //          //       case 5:
        //          //  //       clusterValue.add(new ArrayList<BytesWritable>(170));
        //          //         clusterValue.add(new ArrayList<BytesWritable>(40));
        //          //         break ;
        //          //       case 6:
        //          //       //  clusterValue.add(new ArrayList<BytesWritable>(170));
        //          //         clusterValue.add(new ArrayList<BytesWritable>(40));
        //          //         break ;
        //          //       case 7:
        //          //      //   clusterValue.add(new ArrayList<BytesWritable>(170));
        //          //         clusterValue.add(new ArrayList<BytesWritable>(40));
        //          //         break ;
        //          //       case 8:
        //          //       //  clusterValue.add(new ArrayList<BytesWritable>(1120));
        //          //         clusterValue.add(new ArrayList<BytesWritable>(40));
        //          //         break ;
        //          //       }
        //
        //        }

        preConfigure(job, storageSchema, ETLUtils.getSchema(SerializeUtil.desc.clusterTypes),
            cluster_pages, SerializeUtil.desc.clusterAlgos, SerializeUtil.desc.clusterCodingTypes,
            SerializeUtil.desc.columnsMapping);
      }

      public void preConfigure(JobConf job, DataType[][] originalTypes,
          DataType[][] clusterTypes, int[] pagesizes, Algorithm[] algorithms,
          CodingType[] codings, int[][] columnsMapping) {
        this.columnsMapping = columnsMapping;
        this.codings = codings;
        originalSchema = ETLUtils.getSchema(originalTypes);// //���������������������������������ize������
        numFields = originalSchema.get(0).size();// //������������������
        clusterSchema = ETLUtils.getSchema(clusterTypes);
        numClusters = clusterSchema.size();
        accessors = new ClusterAccessor[numClusters];
        backups = new ClusterAccessor[numClusters];
        rows = new Row[numClusters];// ///
        prevRows = new Row[numClusters];
        maxs = new Row[numClusters];
        mins = new Row[numClusters];
        segMaxs = new Row[numClusters];
        segMins = new Row[numClusters];
        compressors = new Encoder[numClusters];
        vps = new ValPair[numClusters];
        backupVps = new ValPair[numClusters];
        pms = new PageMeta[numClusters];
        pageIds = new int[numClusters];
        startPoss = new int[numClusters];
        numReps = new int[numClusters];
        this.pagesizes=pagesizes;
        this.algorithms=algorithms;
        for (int i = 0; i < numClusters; i++) {
          accessors[i] = new ClusterAccessor();
          accessors[i].init(clusterSchema.get(i));
          backups[i] = new ClusterAccessor(accessors[i]);
          rows[i] = new Row(clusterSchema.get(i));
          prevRows[i] = null;
          //          if (codings[i] != CodingType.MV) {
          //            algorithms[i] = null;
          //          }
          if (Algorithm.NONE ==SerializeUtil.desc.clusterAlgos[i]) {
            algorithms[i] = null;
          }
          if (accessors[i].getFixedLen() > 0) {
            //            compressors[i] = new FixedLenEncoder(pagesizes[i],
            //                accessors[i].getFixedLen()
            //                + (codings[i] == CodingType.RLE ? 2 * Bytes.SIZEOF_INT : 0),
            //                0, algorithms[i]);
            //            compressors[i] = new FixedLenEncoder(pagesizes[i],
            //                accessors[i].getFixedLen(), 0, algorithms[i]);

            compressors[i]=EnDecode.getEncoder(pagesizes[i],accessors[i].getFixedLen(), 0, algorithms[i],codings[i]);
          } else {

            compressors[i] = new VarLenEncoder(pagesizes[i], 0, algorithms[i]);
          }
          compressors[i].reset();

          vps[i] = new ValPair();
          backupVps[i] = new ValPair();

          pms[i] = new PageMeta();
          pms[i].startPos = 0;
          pms[i].numPairs = 0;

          pageIds[i] = 0;

          startPoss[i] = numReps[i] = 0;
        }
        segId = new PageId();
        segId.setSegmentId(Math.random() * 100 + "");
        // String taskId = job.get("mapred.tip.id");
        // String taskId = ETLUtils.getTaskId(job);
        // segId.setSegmentId(taskId);
      }
      public void updateMaxMins(Row oldMax, Row oldMin, Row newMax, Row newMin) {
        int size = oldMax.size();
        for (int i = 0; i < size; i++) {
          Comparable maxCp = (Comparable) newMax.get(i).getObject();
          if (maxCp.compareTo(oldMax.get(i).getObject()) > 0) {
            oldMax.get(i).setObject(newMax.get(i).getPrimitiveObject());
          }
          Comparable minCp = (Comparable) newMin.get(i).getObject();
          if (minCp.compareTo(oldMin.get(i).getObject()) < 0) {
            oldMin.get(i).setObject(newMin.get(i).getPrimitiveObject());
          }
        }
      }
      void outputPage(int i) throws IOException {
        // 1) prepare the page
        byte[] page = compressors[i].getPage();

        int pageLen = compressors[i].getPageLen();

        if (page == null || pageLen <= 0) {
          return;
        }
        out.reset();
        out.writeInt(pageLen);
        out.write(page, 0, pageLen);

        // 2) write page meta
        backupVps[i].data = backups[i].serialize(maxs[i], tmpLength);

        backupVps[i].offset = 0;
        backupVps[i].length = tmpLength[0];
        backupVps[i].write(out);

        backupVps[i].data = backups[i].serialize(mins[i], tmpLength);
        backupVps[i].length = tmpLength[0];

        // write min row
        backupVps[i].write(out);
        out.writeInt(pms[i].startPos);
        out.writeInt(pms[i].numPairs);

        // set max & min row of current segment
        if (segMaxs[i] == null || segMins[i] == null) {
          segMaxs[i] = maxs[i];
          segMins[i] = mins[i];

        } else {

          //    Row.updateMaxMins(segMaxs[i], segMins[i], maxs[i], mins[i]);
          updateMaxMins(segMaxs[i], segMins[i], maxs[i], mins[i]);


        }

        // 3) output the page
        segId.setPageId(pageIds[i]);
        segId.setClusterId(i);


        if(!isInit){
          isInit=true ;
          clusterValue=new ArrayList<List<BytesWritable>>(SerializeUtil.desc.clusterTypes.size());
          for (int b = 0; b < numClusters; b++) {
            clusterValue.add(new ArrayList<BytesWritable>(512));
          }

        }
        clusterValue.get(i).add(new BytesWritable());
        clusterValue.get(i).get(clusterValue.get(i).size() - 1)
        .set(out.getData(), 0, out.getLength());
        sgementSize = sgementSize + cluster_pages[i];

        // 4) reset
        pageIds[i]++;
        pms[i].startPos += pms[i].numPairs;
        pms[i].numPairs = 0;
        maxs[i] = mins[i] = null;
        compressors[i].reset();
      }

      void SegmentMeta() throws IOException {

        for (int i = 0; i < numClusters; i++) {

          if(codings[i]==CodingType.MV){
            outputPage(i);
          }
          else if(codings[i]==CodingType.RunLengthEncodingInt){
            lastPageRunLengthEncodingInt( i);
          }
          else if(codings[i]==CodingType.RunLengthEncodingByte){
            lastPageRunLengthEncodingByte( i);
          }
          else if(codings[i]==CodingType.RunLengthEncodingLong){
            lastPageRunLengthEncodingLong(i);
          }
          else if(codings[i]==CodingType.DeltaBinaryArrayZigZarByte){
            //  lastPageRunLengthEncodingByte( i);
            lastPageDeltaBinaryArraysBitPackingByte(i);
          }
          else if(codings[i]==CodingType.DeltaBinaryBitPackingZigZarInt){
            //   lastPageRunLengthEncodingByte( i);
            lastPageDeltaBinaryBitPackingInt(i);
          }
          else if(codings[i]==CodingType.DeltaBinaryBitPackingZigZarLong){
            //   lastPageRunLengthEncodingByte( i);
            lastPageDeltaBinaryBitPackingLong(i);
          }
          else if(codings[i]==CodingType.DictionaryBitPackingRLEByte){
            //   lastPageRunLengthEncodingByte( i);
            lastPageDictionaryBitPackingRLEZigZarByte(i);
          }
          else if(codings[i]==CodingType.DictionaryBitPackingRLEInt){
            //   lastPageRunLengthEncodingByte( i);
            lastPageDictionaryBitPackingRLEZigZarInt(i);
          }
          else if(codings[i]==CodingType.DictionaryBitPackingRLELong){
            //   lastPageRunLengthEncodingByte( i);
            lastPageDictionaryBitPackingRLEZigZarLong(i);
          }
          else {
            throw new UnsupportedEncodingException("Wrong encoding Type");
          }
        }  // write out the segment infomations
        for (int i = 0; i < numClusters; i++) {
          outputSegmentMeta(i);
        }

      }
      void   lastPageRunLengthEncodingInt(int  i) throws IOException{
        runLengthInteger[i].flush();
        ByteBuffer inBuf = ByteBuffer.allocate(collect[i].buffer.size());
        //         collect.buffer.write(dos, 0, collect.buffer.size());
        byte[] pageBytes=collect[i].buffer.getByteBuffer(inBuf, 0, collect[i].buffer.size()) ;
        collect[i].buffer.clear();
        inBuf.clear();

        vps[i].data=pageBytes ;
        vps[i].offset = 0;
        vps[i].length= pageBytes.length;

        compressors[i].appendPage(vps[i]);

        outputPage(i);
        pageInit[i]=false;
      }
      void   lastPageRunLengthEncodingByte(int  i) throws IOException{
        runLengthByte[i].flush();
        ByteBuffer inBuf = ByteBuffer.allocate(collect[i].buffer.size());
        //         collect.buffer.write(dos, 0, collect.buffer.size());
        byte[] pageBytes=collect[i].buffer.getByteBuffer(inBuf, 0, collect[i].buffer.size()) ;
        collect[i].buffer.clear();
        inBuf.clear();
        vps[i].data=pageBytes ;
        vps[i].offset = 0;
        vps[i].length= pageBytes.length;
        compressors[i].appendPage(vps[i]);
        outputPage(i);
        pageInit[i]=false;
      }
      void   lastPageRunLengthEncodingLong(int  i) throws IOException{
        throw  new  UnsupportedEncodingException("WangMeng  has  not  implement  this  method   on 03.27.2014  yet");
      }
      void   lastPageDeltaBinaryBitPackingInt(int  i) throws IOException{
        BytesInput bi=deltaBianryBitPackingInt[i].getBytes() ;
        byte[] pageBytes=bi.getBufferSize() ;
        vps[i].data=pageBytes ;
        vps[i].offset = 0;
        vps[i].length= pageBytes.length;
        try {
          compressors[i].appendPage(vps[i]);
        } catch (IOException e) {
          e.printStackTrace();
        }

        try {
          outputPage(i);
        } catch (IOException e) {
          e.printStackTrace();
        }
        pageInit[i]=false;
      }
      void   lastPageDeltaBinaryArraysBitPackingByte(int  i) throws IOException{
        BytesInput  bi=deltaByteArrayWriter[i].getBytes();
        byte[] pageBytes=bi.getBufferSize() ;
        vps[i].data=pageBytes ;
        vps[i].offset = 0;
        vps[i].length= pageBytes.length;
        try {
          compressors[i].appendPage(vps[i]);
        } catch (IOException e) {
          e.printStackTrace();
        }

        try {
          outputPage(i);
        } catch (IOException e) {
          e.printStackTrace();
        }
        pageInit[i]=false;
      }
      void   lastPageDeltaBinaryBitPackingLong(int  i) throws IOException{
        throw  new  UnsupportedEncodingException("WangMeng  has  not  implement  this  method   on 03.27.2014  yet");
      }
      void   lastPageDictionaryBitPackingRLEZigZarByte(int  i) throws IOException{
        BytesInput sbi= dictionaryBitPackingRLEZigZarByte[i].getBytes();
        //  long tmp=cw.WriteDictionaryToDisk(s);
        byte[]   dictionaryBuffer=dictionaryBitPackingRLEZigZarByte[i].getDictionaryBuffer();
        //    long tmp1=  sbi.writeToDisk(str);
        byte[]    dictionaryID=sbi.getBufferSize() ;
        DataOutputBuffer  dob=new DataOutputBuffer() ;
        dob.writeInt(dictionaryBuffer.length);
        dob.write(dictionaryBuffer, 0, dictionaryBuffer.length);
        dob.write(dictionaryID, 0, dictionaryID.length);
        byte[] pageBytes=dob.getData() ;
        dob.close();
        vps[i].data=pageBytes ;
        vps[i].offset = 0;
        vps[i].length= pageBytes.length;
        try {
          compressors[i].appendPage(vps[i]);
        } catch (IOException e) {
          e.printStackTrace();
        }

        try {
          outputPage(i);
        } catch (IOException e) {
          e.printStackTrace();
        }
        pageInit[i]=false;
      }
      void   lastPageDictionaryBitPackingRLEZigZarInt(int  i) throws IOException{

        BytesInput sbi= dictionaryBitPackingRLEZigZarInt[i].getBytes();
        //  long tmp=cw.WriteDictionaryToDisk(s);
        byte[]   dictionaryBuffer=dictionaryBitPackingRLEZigZarInt[i].getDictionaryBuffer();
        //    long tmp1=  sbi.writeToDisk(str);
        byte[]    dictionaryID=sbi.getBufferSize() ;

        DataOutputBuffer  dob=new DataOutputBuffer() ;
        dob.writeInt(dictionaryBuffer.length);
        dob.write(dictionaryBuffer, 0, dictionaryBuffer.length);
        dob.write(dictionaryID, 0, dictionaryID.length);
        byte[] pageBytes=dob.getData() ;
        dob.close();
        vps[i].data=pageBytes ;
        vps[i].offset = 0;
        vps[i].length= pageBytes.length;
        try {
          compressors[i].appendPage(vps[i]);
        } catch (IOException e) {
          e.printStackTrace();
        }

        try {
          outputPage(i);
        } catch (IOException e) {
          e.printStackTrace();
        }
        pageInit[i]=false;
      }
      void   lastPageDictionaryBitPackingRLEZigZarLong(int  i) throws IOException{
        throw  new  UnsupportedEncodingException("WangMeng  has  not  implement  this  method   on 03.27.2014  yet");
      }
      private void outputSegmentMeta(int i)
          throws IOException {
        out.reset();
        // 1) write segment meta
        backupVps[i].data = backups[i].serialize(segMaxs[i], tmpLength);

        backupVps[i].offset = 0;
        backupVps[i].length = tmpLength[0];

        backupVps[i].write(out);


        backupVps[i].data = backups[i].serialize(segMins[i], tmpLength);
        backupVps[i].length = tmpLength[0];

        // write min row
        backupVps[i].write(out);

        out.writeInt(0); // start pos of a segment is zero
        out.writeInt(pms[i].startPos);




        // 2) output the page
        segId.setPageId(-1); // meta data page is -1
        segId.setClusterId(i);
        outValue.set(out.getData(), 0, out.getLength());
        clusterValue.get(i).add(new BytesWritable());
        clusterValue.get(i).get(clusterValue.get(i).size() - 1)
        .set(out.getData(), 0, out.getLength());
        sgementSize = sgementSize + cluster_pages[i];
        //        segMaxs[i]=null ;
        //        segMins[i]=null ;
        pms[i].startPos =0;
        pms[i].numPairs = 0;
        /////////////////////////////////
        if (accessors[i].getFixedLen() > 0) {
          //          compressors[i] = new FixedLenEncoder(pagesizes[i],
          //              accessors[i].getFixedLen()
          //              + (codings[i] == CodingType.RLE ? 2 * Bytes.SIZEOF_INT : 0),
          //              0, algorithms[i]);
          compressors[i]=EnDecode.getEncoder(pagesizes[i],
              accessors[i].getFixedLen(), 0, algorithms[i],codings[i]);
          compressors[i].reset();
        } else {
          compressors[i] = new VarLenEncoder(pagesizes[i], 0, algorithms[i]);
          compressors[i].reset();
        }
        maxs[i] = mins[i] = null;
        segMaxs[i]=null ;
        segMins[i]=null ;
        pms[i].startPos =0;
        pms[i].numPairs = 0;
        backups[i] = new ClusterAccessor(accessors[i]);
        vps[i] = new ValPair();
        backupVps[i] = new ValPair();

        pms[i] = new PageMeta();

        pageIds[i] = 0;
        //
        startPoss[i] = numReps[i] = 0;
      }

      void passValue(PageId key, ArrayList<List<BytesWritable>> segmentValue2)
          throws IOException {
        int m = 0;
        // SerializeUtil.writer.beginSegment();
        // SerializeUtil.writer.beginCluster();
        beginSegment();
        beginCluster();
        for (int i = 0; i < segmentValue2.size(); i++) {
          for (int j = -1; j < segmentValue2.get(i).size() - 1; j++) {
            PageMeta pm = new PageMeta();

            BytesWritable bw;
            if (j != -1) {
              bw = (BytesWritable) segmentValue2.get(i).get(j);
              in.reset(bw.getBytes(), 0, bw.getLength());
            }
            else {
              bw = (BytesWritable) segmentValue2.get(i).get(segmentValue2.get(i).size() - 1);
              in.reset(bw.getBytes(), 0, bw.getLength());

            }
            if (m == 0) {
              pm.readFields(in);
              // SerializeUtil.writer.addSegmentMeta(i, pm);
              addSegmentMeta(i, pm);
              // SerializeUtil.writer.addSegmentMeta(key.getClusterId(), pm);
              m++;
            }
            else {
              int length = in.readInt();
              in.skip(length);
              pm.readFields(in);
              // SerializeUtil.writer.append(bw.getBytes(), Bytes.SIZEOF_INT, length, pm);
              Segappend(bw.getBytes(), Bytes.SIZEOF_INT, length, pm);
            }
          }
          // SerializeUtil.writer.finishCluster();
          finishCluster();
          m = 0;
          if (i < segmentValue2.size() - 1) {
            // SerializeUtil.writer.beginCluster();
            beginCluster();
          }
        }
        // SerializeUtil.writer.finishSegment();
        finishSegment();
        // outputStream.flush();
        // outputStream.sync();
        // segmentValue2.clear();
        // segmentValue2=null ;
      }
      public  void  runLengthEncodingInt(int i){
        if( pageInit[i]==false){
          //      TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
          collect[i] = new TestInStream.OutputCollector();
          // fixedEncoding=  (FixedLenEncoder)compressors[i]   ;
          compressors[i].reset();
          try {
            runLengthInteger[i] = new RunLengthIntegerWriter(new OutStream("test", 1000, null, collect[i]), true);
          } catch (IOException e) {
            e.printStackTrace();
          }
          pageInit[i]=true;
        }

        if(compressors[i].dataOffset + compressors[i].valueLen < compressors[i].pageCapacity){
          //    if(collect.buffer.size()< fixedEncoding.pageCapacity-12-4){
          //   rleEncoding.write(Long.parseLong((String)(rows[i].getValue(0))));
          try {
            runLengthInteger[i].write(((Integer)(rows[i].getValue(0))).intValue());
          } catch (IOException e) {
            e.printStackTrace();
          }
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;



          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }

          pms[i].numPairs++;
        }
        else{
          try {
            runLengthInteger[i].write(((Integer)(rows[i].getValue(0))).intValue());
          } catch (IOException e) {
            e.printStackTrace();
          }
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;



          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }

          pms[i].numPairs++;
          try {
            runLengthInteger[i].flush();
          } catch (IOException e) {
            e.printStackTrace();
          }
          ByteBuffer inBuf = ByteBuffer.allocate(collect[i].buffer.size());
          //         collect.buffer.write(dos, 0, collect.buffer.size());
          byte[] pageBytes=collect[i].buffer.getByteBuffer(inBuf, 0, collect[i].buffer.size()) ;
          collect[i].buffer.clear();
          inBuf.clear();

          vps[i].data=pageBytes ;
          vps[i].offset = 0;
          vps[i].length= pageBytes.length;
          try {
            compressors[i].appendPage(vps[i]);
          } catch (IOException e) {
            e.printStackTrace();
          }

          try {
            outputPage(i);
          } catch (IOException e) {
            e.printStackTrace();
          }

          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          LOG.info("dataoffset  "+compressors[i].dataOffset);
          LOG.info("numPairs  "+compressors[i].numPairs);
          LOG.info("startPos  "+compressors[i].startPos);


          //      pms[i].numPairs++;
          pageInit[i]=false;
        }


      }

      public  void runLengthEncodingByte(int i){
        //        TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
        //        RunLengthByteWriter out = new RunLengthByteWriter(new OutStream("test", 1000, codec, collect));


        if( pageInit[i]==false){
          //      TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
          collect[i] = new TestInStream.OutputCollector();
          // fixedEncoding=  (FixedLenEncoder)compressors[i]   ;
          compressors[i].reset();
          //     RunLengthByteWriter oust = new RunLengthByteWriter(new OutStream("test", 1000, null, collect[i]));
          try {
            runLengthByte[i] = new RunLengthByteWriter(new OutStream("test", 1000, null, collect[i]));
          } catch (IOException e) {
            e.printStackTrace();
          }
          pageInit[i]=true;
        }

        if(compressors[i].dataOffset + compressors[i].valueLen < compressors[i].pageCapacity){
          //    if(collect.buffer.size()< fixedEncoding.pageCapacity-12-4){
          //   rleEncoding.write(Long.parseLong((String)(rows[i].getValue(0))));
          try {
            runLengthByte[i].write(((Byte)(rows[i].getValue(0))).byteValue());
          } catch (IOException e) {
            e.printStackTrace();
          }
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;



          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }

          pms[i].numPairs++;
        }
        else{
          try {
            runLengthByte[i].write(((Byte)(rows[i].getValue(0))).byteValue());
          } catch (IOException e) {
            e.printStackTrace();
          }
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;



          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }

          pms[i].numPairs++;

          try {
            runLengthByte[i].flush();
          } catch (IOException e) {
            e.printStackTrace();
          }
          ByteBuffer inBuf = ByteBuffer.allocate(collect[i].buffer.size());
          //         collect.buffer.write(dos, 0, collect.buffer.size());
          byte[] pageBytes=collect[i].buffer.getByteBuffer(inBuf, 0, collect[i].buffer.size()) ;

          collect[i].buffer.clear();
          inBuf.clear();

          vps[i].data=pageBytes ;
          vps[i].offset = 0;
          vps[i].length= pageBytes.length;
          try {
            compressors[i].appendPage(vps[i]);
          } catch (IOException e) {
            e.printStackTrace();
          }

          try {
            outputPage(i);
          } catch (IOException e) {
            e.printStackTrace();
          }

          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          //    pms[i].numPairs++;
          pageInit[i]=false;
        }


      }

      //
      //    case  RunLengthEncodingLong: runLengthEncodingLong(i); break ;
      //    case  DeltaBinaryArrayZigZarByte :deltaBinaryArrayZigZarByte(i); break ;
      //    case DeltaBinaryBitPackingZigZarInt :deltaBinaryBitPackingZigZarInt(i); break ;
      //    case DeltaBinaryBitPackingZigZarLong :deltaBinaryBitPackingZigZarLong(i); break ;
      //    case DictionaryBitPackingRLEByte:dictionaryBitPackingRLEByte(i);break ;
      //    case DictionaryBitPackingRLEInt:dictionaryBitPackingRLEint(i);break ;
      //    case DictionaryBitPackingRLELong:dictionaryBitPackingRLELong(i);break ;
      public  void runLengthEncodingLong(int i) throws UnsupportedEncodingException{
        throw  new  UnsupportedEncodingException("WangMeng  has  not  implement  this  method   on 03.27.2014  yet");
      }
      public  void deltaBinaryArrayZigZarByte(int i) throws IOException{
        //        DeltaByteArrayWriter  delta=new   DeltaByteArrayWriter((int)file.length()/10);
        //        for(int i=0; i < fileLong; i++) {
        //          //  delta.writeBytes(Binary.fromString(""+data[i]));
        //          //  delta.writeBytes(data[i]);
        //          delta.writeBytes(Binary.fromString(""+data[i]));
        //        }
        //        BytesInput  bi=delta.getBytes();



        if( pageInit[i]==false){
          //          int blockSize = 128;
          //          int miniBlockNum = 4;
          deltaByteArrayWriter[i]=new DeltaByteArrayWriter( pagesizes[i]/4);
          // fixedEncoding=  (FixedLenEncoder)compressors[i]   ;
          compressors[i].reset();
          pageInit[i]=true;
        }
        if(compressors[i].dataOffset + compressors[i].valueLen < compressors[i].pageCapacity){
          //    if(collect.buffer.size()< fixedEncoding.pageCapacity-12-4){
          //   rleEncoding.write(Long.parseLong((String)(rows[i].getValue(0))));
          // deltaBianryBitPackingInt[i].writeInteger(((Byte)(rows[i].getValue(0))).byteValue());

          deltaByteArrayWriter[i].writeBytes(Binary.fromString(""+((Byte)(rows[i].getValue(0))).byteValue()));
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;
          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }

          pms[i].numPairs++;
        }
        else{
          deltaByteArrayWriter[i].writeBytes(Binary.fromString(""+((Byte)(rows[i].getValue(0))).byteValue()));
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;
          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          pms[i].numPairs++;
          BytesInput  bi=deltaByteArrayWriter[i].getBytes();
          byte[] pageBytes=bi.getBufferSize() ;
          vps[i].data=pageBytes ;
          vps[i].offset = 0;
          vps[i].length= pageBytes.length;
          try {
            compressors[i].appendPage(vps[i]);
          } catch (IOException e) {
            e.printStackTrace();
          }

          try {
            outputPage(i);
          } catch (IOException e) {
            e.printStackTrace();
          }

          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          LOG.info("dataoffset  "+compressors[i].dataOffset);
          LOG.info("numPairs  "+compressors[i].numPairs);
          LOG.info("startPos  "+compressors[i].startPos);


          //  pms[i].numPairs++;
          pageInit[i]=false;
        }
      }
      public  void deltaBinaryBitPackingZigZarInt(int i) throws IOException{
        // ValuesWriter rle = new RunLengthBitPackingHybridValuesWriter(32, 100);
        if( pageInit[i]==false){
          int blockSize = 128;
          int miniBlockNum = 4;
          deltaBianryBitPackingInt[i] = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, pagesizes[i]/4);
          // fixedEncoding=  (FixedLenEncoder)compressors[i]   ;
          compressors[i].reset();
          pageInit[i]=true;
        }
        if(compressors[i].dataOffset + compressors[i].valueLen < compressors[i].pageCapacity){
          //    if(collect.buffer.size()< fixedEncoding.pageCapacity-12-4){
          //   rleEncoding.write(Long.parseLong((String)(rows[i].getValue(0))));
          deltaBianryBitPackingInt[i].writeInteger(((Integer)(rows[i].getValue(0))).intValue());
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;
          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }

          pms[i].numPairs++;
        }
        else{
          deltaBianryBitPackingInt[i].writeInteger(((Integer)(rows[i].getValue(0))).intValue());
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;
          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          pms[i].numPairs++;
          BytesInput bi=deltaBianryBitPackingInt[i].getBytes() ;
          byte[] pageBytes=bi.getBufferSize() ;
          vps[i].data=pageBytes ;
          vps[i].offset = 0;
          vps[i].length= pageBytes.length;
          try {
            compressors[i].appendPage(vps[i]);
          } catch (IOException e) {
            e.printStackTrace();
          }

          try {
            outputPage(i);
          } catch (IOException e) {
            e.printStackTrace();
          }

          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          LOG.info("dataoffset  "+compressors[i].dataOffset);
          LOG.info("numPairs  "+compressors[i].numPairs);
          LOG.info("startPos  "+compressors[i].startPos);


          //   pms[i].numPairs++;
          pageInit[i]=false;
        }
      }
      public  void deltaBinaryBitPackingZigZarLong(int i) throws UnsupportedEncodingException{
        throw new  UnsupportedEncodingException("now I  have  not  implement   this  method   for  twitter.Parquet  do  not  implement  this  just now")  ;
      }

      //      final OnlyDictionaryValuesWriter.PlainBinaryDictionaryValuesWriter cw = new OnlyDictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(Integer.MAX_VALUE, fileLong);
      //      for (int i = 0; i < fileLong; i++) {
      //        cw.writeBytes(Binary.fromString("" +initbytes[i]));
      //      }
      //      BytesInput  bi=cw.getBytes() ;
      //      encodingWriteTime=System.currentTimeMillis() ;
      //
      //      long tmp= cw.WriteDictionaryToDisk(s);
      //      long tmp0=bi.writeToDisk(str);
      //


      public  void dictionaryBitPackingRLEByte(int i) throws IOException{
        if( pageInit[i]==false){
          //   deltaBianryBitPackingInt[i] = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, pagesizes[i]/4);
          dictionaryBitPackingRLEZigZarByte[i]= new PlainBinaryDictionaryValuesWriter(Integer.MAX_VALUE, pagesizes[i]);
          // fixedEncoding=  (FixedLenEncoder)compressors[i]   ;
          compressors[i].reset();
          pageInit[i]=true;
        }
        if(compressors[i].dataOffset + compressors[i].valueLen < compressors[i].pageCapacity){
          //    if(collect.buffer.size()< fixedEncoding.pageCapacity-12-4){
          //   rleEncoding.write(Long.parseLong((String)(rows[i].getValue(0))));
          //  dictionaryBitPackingRLEZigZarInt[i].writeInteger(((Byte)(rows[i].getValue(0))).byteValue());
          dictionaryBitPackingRLEZigZarByte[i].writeBytes(Binary.fromString("" +((Byte)(rows[i].getValue(0))).byteValue()));
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;
          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }

          pms[i].numPairs++;
        }
        else{
          dictionaryBitPackingRLEZigZarByte[i].writeBytes(Binary.fromString("" +((Byte)(rows[i].getValue(0))).byteValue()));
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;
          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          pms[i].numPairs++;

          BytesInput sbi= dictionaryBitPackingRLEZigZarByte[i].getBytes();
          //  long tmp=cw.WriteDictionaryToDisk(s);
          byte[]   dictionaryBuffer=dictionaryBitPackingRLEZigZarByte[i].getDictionaryBuffer();
          //    long tmp1=  sbi.writeToDisk(str);
          byte[]    dictionaryID=sbi.getBufferSize() ;
          DataOutputBuffer  dob=new DataOutputBuffer() ;
          dob.writeInt(dictionaryBuffer.length);
          dob.write(dictionaryBuffer, 0, dictionaryBuffer.length);
          dob.write(dictionaryID, 0, dictionaryID.length);
          byte[] pageBytes=dob.getData() ;
          dob.close();
          vps[i].data=pageBytes ;
          vps[i].offset = 0;
          vps[i].length= pageBytes.length;
          try {
            compressors[i].appendPage(vps[i]);
          } catch (IOException e) {
            e.printStackTrace();
          }

          try {
            outputPage(i);
          } catch (IOException e) {
            e.printStackTrace();
          }

          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          LOG.info("dataoffset  "+compressors[i].dataOffset);
          LOG.info("numPairs  "+compressors[i].numPairs);
          LOG.info("startPos  "+compressors[i].startPos);


          //   pms[i].numPairs++;
          pageInit[i]=false;
        }

      }
      public  void dictionaryBitPackingRLEInt(int i) throws IOException{

        if( pageInit[i]==false){
          //   deltaBianryBitPackingInt[i] = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, pagesizes[i]/4);
          dictionaryBitPackingRLEZigZarInt[i]= new PlainIntegerDictionaryValuesWriter(Integer.MAX_VALUE, pagesizes[i]);
          // fixedEncoding=  (FixedLenEncoder)compressors[i]   ;
          compressors[i].reset();
          pageInit[i]=true;
        }
        if(compressors[i].dataOffset + compressors[i].valueLen < compressors[i].pageCapacity){
          //    if(collect.buffer.size()< fixedEncoding.pageCapacity-12-4){
          //   rleEncoding.write(Long.parseLong((String)(rows[i].getValue(0))));
          dictionaryBitPackingRLEZigZarInt[i].writeInteger(((Integer)(rows[i].getValue(0))).intValue());
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;
          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }

          pms[i].numPairs++;
        }
        else{
          dictionaryBitPackingRLEZigZarInt[i].writeInteger(((Integer)(rows[i].getValue(0))).intValue());
          compressors[i].numPairs++;
          compressors[i].dataOffset += compressors[i].valueLen;
          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          pms[i].numPairs++;
          BytesInput sbi= dictionaryBitPackingRLEZigZarInt[i].getBytes();
          //  long tmp=cw.WriteDictionaryToDisk(s);
          byte[]   dictionaryBuffer=dictionaryBitPackingRLEZigZarInt[i].getDictionaryBuffer();
          //    long tmp1=  sbi.writeToDisk(str);
          byte[]    dictionaryID=sbi.getBufferSize() ;
          DataOutputBuffer  dob=new DataOutputBuffer() ;
          dob.writeInt(dictionaryBuffer.length);
          dob.write(dictionaryBuffer, 0, dictionaryBuffer.length);
          dob.write(dictionaryID, 0, dictionaryID.length);
          byte[] pageBytes=dob.getData() ;
          dob.close();
          vps[i].data=pageBytes ;
          vps[i].offset = 0;
          vps[i].length= pageBytes.length;
          try {
            compressors[i].appendPage(vps[i]);
          } catch (IOException e) {
            e.printStackTrace();
          }

          try {
            outputPage(i);
          } catch (IOException e) {
            e.printStackTrace();
          }

          if (maxs[i] == null || mins[i] == null) {
            maxs[i] = rows[i].duplicate();
            mins[i] = rows[i].duplicate();
          } else {
            rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
          }
          LOG.info("dataoffset  "+compressors[i].dataOffset);
          LOG.info("numPairs  "+compressors[i].numPairs);
          LOG.info("startPos  "+compressors[i].startPos);


          //  pms[i].numPairs++;
          pageInit[i]=false;
        }


      }
      public  void dictionaryBitPackingRLELong(int i) throws UnsupportedEncodingException{
        throw  new  UnsupportedEncodingException("WangMeng  has  not  implement  this  method   on 03.27.2014  yet");
      }

      public   void append(Writable val) throws IOException {
        RowMap  rowMap = (RowMap) val;

        // System.out.println(originalRow) ;
        for (int i = 0; i < numClusters; i++) {
          rows[i]=rowMap.row[i];
          if(codings[i]!=CodingType.MV){
            switch   (codings[i]){
            case  RunLengthEncodingByte: runLengthEncodingByte(i); break ;
            case  RunLengthEncodingInt: runLengthEncodingInt(i); break ;
            case  RunLengthEncodingLong: runLengthEncodingLong(i); break ;
            case  DeltaBinaryArrayZigZarByte :deltaBinaryArrayZigZarByte(i); break ;
            case DeltaBinaryBitPackingZigZarInt :deltaBinaryBitPackingZigZarInt(i); break ;
            case DeltaBinaryBitPackingZigZarLong :deltaBinaryBitPackingZigZarLong(i); break ;
            case DictionaryBitPackingRLEByte:dictionaryBitPackingRLEByte(i);break ;
            case DictionaryBitPackingRLEInt:dictionaryBitPackingRLEInt(i);break ;
            case DictionaryBitPackingRLELong:dictionaryBitPackingRLELong(i);break ;
            default :  throw  new  UnsupportedEncodingException()  ;
            }


            //            if (codings[i] == CodingType.RunLengthEncodingInt) {
            //
            //              if( pageInit[i]==false){
            //                //      TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
            //                collect[i] = new TestInStream.OutputCollector();
            //                // fixedEncoding=  (FixedLenEncoder)compressors[i]   ;
            //                compressors[i].reset();
            //                rleEncoding[i] = new RunLengthIntegerWriter(new OutStream("test", 1000, null, collect[i]), true);
            //                pageInit[i]=true;
            //              }
            //
            //              if(compressors[i].dataOffset + compressors[i].valueLen < compressors[i].pageCapacity){
            //                //    if(collect.buffer.size()< fixedEncoding.pageCapacity-12-4){
            //                //   rleEncoding.write(Long.parseLong((String)(rows[i].getValue(0))));
            //                rleEncoding[i].write(((Integer)(rows[i].getValue(0))).intValue());
            //                compressors[i].numPairs++;
            //                compressors[i].dataOffset += compressors[i].valueLen;
            //
            //
            //
            //                if (maxs[i] == null || mins[i] == null) {
            //                  maxs[i] = rows[i].duplicate();
            //                  mins[i] = rows[i].duplicate();
            //                } else {
            //                  rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
            //                }
            //
            //                pms[i].numPairs++;
            //              }
            //              else{
            //                rleEncoding[i].write(((Integer)(rows[i].getValue(0))).intValue());
            //                compressors[i].numPairs++;
            //                compressors[i].dataOffset += compressors[i].valueLen;
            //
            //
            //
            //                if (maxs[i] == null || mins[i] == null) {
            //                  maxs[i] = rows[i].duplicate();
            //                  mins[i] = rows[i].duplicate();
            //                } else {
            //                  rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
            //                }
            //
            //                pms[i].numPairs++;
            //                System.out.println("dataoffset  "+compressors[i].dataOffset);
            //                System.out.println("585 numPairs  "+compressors[i] .numPairs);
            //                System.out.println("startPos  "+compressors[i] .startPos);
            //                System.out.println("547 collect.buffer.size()  "+collect[i].buffer.size() );
            //                System.out.println("547  fixedEncoding.dataOffset  "+compressors[i].dataOffset );
            //                System.out.println("547 fixedEncoding.valueLen       "+compressors[i].valueLen );
            //                System.out.println("547 fixedEncoding.pageCapacity    "+compressors[i].pageCapacity );
            //                rleEncoding[i].flush();
            //                ByteBuffer inBuf = ByteBuffer.allocate(collect[i].buffer.size());
            //                //         collect.buffer.write(dos, 0, collect.buffer.size());
            //                byte[] pageBytes=collect[i].buffer.getByteBuffer(inBuf, 0, collect[i].buffer.size()) ;
            //                LOG.info("rlecodingSize "+pageBytes.length);
            //                System.out.println("rlecodingSize "+pageBytes.length);
            //                collect[i].buffer.clear();
            //                inBuf.clear();
            //
            //                vps[i].data=pageBytes ;
            //                vps[i].offset = 0;
            //                vps[i].length= pageBytes.length;
            //                compressors[i].appendPage(vps[i]);
            //
            //                outputPage(i);
            //
            //                if (maxs[i] == null || mins[i] == null) {
            //                  maxs[i] = rows[i].duplicate();
            //                  mins[i] = rows[i].duplicate();
            //                } else {
            //                  rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
            //                }
            //                LOG.info("dataoffset  "+compressors[i].dataOffset);
            //                LOG.info("numPairs  "+compressors[i].numPairs);
            //                LOG.info("startPos  "+compressors[i].startPos);
            //
            //
            //                pms[i].numPairs++;
            //                pageInit[i]=false;
            //              }
            //
            //            }
            //////////////////////////////////////////////////////////////////////////////////

          }
          else { // if (codings[i] == CodingType.MV)
            // try to write the current row
            vps[i].data = accessors[i].serialize(rows[i], tmpLength);
            vps[i].offset = 0;
            vps[i].length = tmpLength[0];
            vps[i].pos = pms[i].startPos + pms[i].numPairs;
            while (!compressors[i].append(vps[i])) {
              outputPage(i);
            }
            // update page meta
            if (maxs[i] == null || mins[i] == null) {
              maxs[i] = rows[i].duplicate();
              mins[i] = rows[i].duplicate();
            } else {
              rows[i].compareAndSetMaxMin(maxs[i], mins[i]);
            }

            pms[i].numPairs++;
          }
          // ///very important ,jude whether write to disk or not
          //   if (i == (numClusters - 1) && (sgementSize> (536870912-numClusters*131072)) ) {
          if ((sgementSize>SegmentSize) && i == (numClusters - 1)  ) {

            SegmentMeta();

            passValue(segId, clusterValue);
            //   System.out.println("504  "+ outputStream.getPos());
            outputStream.flush();

            segId = new PageId();
            //  segId.setSegmentId(Math.random() * 100 + "");
            pageIdCount++ ;
            segId.setSegmentId(""+ pageIdCount);
            //            for(int l=0 ;l< clusterValue.size();l++){
            //              //          for(int j=0;j<clusterValue.get(i).size();j++){
            //              //            clusterValue.get(i).get(j).
            //              //          }
            //              clusterValue.get(l).clear();
            //            }

            //      clusterValue.clear();
            //   segmentValue.clear();
            sgementSize = 0;
            isInit=false ;
            //  }
          }

        }

        //       rowMap=null ;
        //      val=null;

      }

      void close() throws IOException {
        SegmentMeta();
        // for (int l = 0; l < numClusters; l++) {
        // segmentValue.put(l, clusterValue.get(l));
        //
        // }
        passValue(segId, clusterValue);
        for (int k = 0; k < numClusters; k++) {
          pageIds[k] = 0;
          pms[k].startPos = 0;
          pms[k].numPairs = 0;
          maxs[k] = mins[k] = null;
        }
        segId = new PageId();
        segId.setSegmentId(Math.random() * 100 + "");
        clusterValue.clear();
        // segmentValue.clear();
        sgementSize = 0;
      }
    }

    // FileSystem stream to write on.
    private FSDataOutputStream outputStream;
    // True if we opened the <code>outputStream</code> (and so will close it).
    private boolean closeOutputStream;

    // Name for this object used when logging or in toString. Is either
    // the result of a toString on stream or else toString of passed file Path.
    private String name;
    private boolean TabConfiInit = false;
    private final  ArrayList<Long> segOffsets = new ArrayList<Long>();
    private final   ArrayList<Long> segLengths = new ArrayList<Long>();
    private final  ArrayList<Long> segPMSOffsets = new ArrayList<Long>();
    // Segment Meta
    private PageMeta[] curSegMetas;
    private final List<PageMeta[]> segMetasList = new ArrayList<PageMeta[]>();
    // Page Meta Section
    private   PageMetaSection pms;
    private  PageMetaList[] pageMetaLists;
    private List<PageMeta> curPageMetaList;
    private List<Long> curPMOffsetList;
    private  TabConfig tabConfig;
    // Cluster Offset in current segment
    private long[] clusterOffsetInCurSegment;
    // May be null if we were passed a stream.
    private Path path = null;
    private Path tmpPath=null ;
    private Path tmpoutputPath=null ;
    private int curClusterIdx;
    private int curSegIdx;
    private int numClusters;
    private final boolean withPageMeta;
    private FileSystem fs =null ;

    /**
     * Constructor that takes a Path.
     *
     * @param fs
     * @param path
     * @param columns
     * @throws IOException
     */
    public Writer(FileSystem fs, Path tmpPath, Path path, Path tmpoutputPath,
        List<List<DataType>> columns)
            throws IOException {
      this(fs, tmpPath, path, tmpoutputPath, columns, true);
    }

    /**
     * Constructor that takes a Path
     *
     * @param fs
     * @param path
     * @param columns
     * @param withPageMeta
     * @throws IOException
     */
    public Writer(FileSystem fs, Path tmpPath, Path path, Path finalOutPath,
        List<List<DataType>> columns,
        boolean withPageMeta)
            throws IOException {
      this(fs.create(tmpPath, true, fs.getConf().getInt("io.file.buffer.size", 4096),
          fs.getDefaultReplication(), fs.getDefaultBlockSize()), columns, withPageMeta);
      fs.setVerifyChecksum(true);
      closeOutputStream = true;
      name = path.toString();
      this.path = path;
      this.tmpPath = tmpPath;
      this.fs = fs;
      this.tmpoutputPath = tmpoutputPath;
    }

    /**
     * Constructor that takes a stream.
     *
     * @param ostream
     *          Stream to use.
     * @param columns
     * @param withPageMeta
     * @throws IOException
     */
    public Writer(final FSDataOutputStream ostream, List<List<DataType>> columns,
        boolean withPageMeta)
            throws IOException {
      // LOG.debug("create a writer...");
      this.outputStream = ostream;
      this.closeOutputStream = false;
      this.name = this.outputStream.toString();
      this.withPageMeta = withPageMeta;
      init(columns);
    }

    private void init(List<List<DataType>> columns) {
      if (columns != null) {
        numClusters = columns.size();
        // LOG.debug("Init " + numClusters + " clusters.");

        pms = new PageMetaSection(withPageMeta);
        pageMetaLists = new PageMetaList[numClusters];
        for (int i = 0; i < numClusters; i++) {
          pageMetaLists[i] = new PageMetaList(withPageMeta);
          pageMetaLists[i].setMetaList(new ArrayList<PageMeta>(),
              new ArrayList<Long>());
        }

        clusterOffsetInCurSegment = new long[numClusters];
      }

      curSegIdx = 0;
    }

    public void append(Writable r) throws IOException {
      if (TabConfiInit == false) {
        tabConfig = new TabConfig();
        tabConfig.configure(SerializeUtil.jc, SerializeUtil.tableProperties);
        TabConfiInit = true;
        tabConfig.append(r);
      } else {
        tabConfig.append(r);
      }
    }

    private void resetPageMetaSection() {
      for (PageMetaList pageMetaList : pageMetaLists) {
        pageMetaList.getMetaList().clear();
        pageMetaList.getOffsetList().clear();
      }
    }

    public void addSegmentMeta(int clusterId, final PageMeta pm) {
      curSegMetas[clusterId] = pm;
    }

    public void beginSegment() throws IOException {
      LOG.info("Begin a new Segment from position " + outputStream.getPos());

      // And the segment start offset;
      segOffsets.add(outputStream.getPos());

      // reinit the arguments of a segment
      curClusterIdx = 0;

      if (withPageMeta) {
        curSegMetas = new PageMeta[numClusters];
      }
    }

    public  void finishSegment() throws IOException {
      LOG.info("Finish data section in a Segment at position " + outputStream.getPos());
      segPMSOffsets.add(outputStream.getPos());
      // write the pagemeta section
      LOG.info("And write its page meta section.");
      pms.setPageMetaLists(pageMetaLists);
      pms.write(outputStream);
      // write the cluter index
      LOG.info("And write its clusters' offsets.");
      for (long l : clusterOffsetInCurSegment) {
        outputStream.writeLong(l);
      }

      // record the length of the segment
      long length = outputStream.getPos() - segOffsets.get(curSegIdx);
      LOG.info("This segment  length is " + length);
      segLengths.add(length);

      if (withPageMeta) {
        segMetasList.add(curSegMetas);
        curSegMetas = null;
      }
      resetPageMetaSection();
      pageMetaLists = new PageMetaList[numClusters];
      for (int i = 0; i < numClusters; i++) {
        pageMetaLists[i] = new PageMetaList(withPageMeta);
        pageMetaLists[i].setMetaList(new ArrayList<PageMeta>(),
            new ArrayList<Long>());
      }


      curSegIdx++;
    }
    /**
     * @return Path or null if we were passed a stream rather than a Path.
     */
    public Path getPath() {
      return this.path;
    }

    @Override
    public String toString() {
      return "writer=" + this.name;
    }

    public void beginCluster() throws IOException {
      LOG.info("Begin a new Cluster from position " + outputStream.getPos());

      curPageMetaList = this.pageMetaLists[curClusterIdx].getMetaList();
      curPMOffsetList = this.pageMetaLists[curClusterIdx].getOffsetList();

      this.clusterOffsetInCurSegment[curClusterIdx] = outputStream.getPos();
    }

    public void finishCluster() {
      LOG.info("Finish Cluster " + curClusterIdx + ".");
      LOG.info("Finish a Cluster while writing " + curPageMetaList.size() + " pages.");
      curClusterIdx++;
    }

    public void Segappend(final byte[] page, final int offset,
        final int length, final PageMeta pm) throws IOException {
      curPMOffsetList.add(outputStream.getPos());
      outputStream.write(page, offset, length);
      curPageMetaList.add(pm);
    }


    public void fileClose() throws IOException {
      // outputStream.flush();
      // outputStream.sync();
      if (outputStream == null) {
        return;
      }

      LOG.info("Finish the segment file by writing its segments' index at position "
          + outputStream.getPos() + " .");
      LOG.info("Total segments are " + curSegIdx);
      long segIdxOffset = outputStream.getPos();

      // Write out the segment index
      for (int i = 0; i < curSegIdx; i++) {
        outputStream.writeLong(segOffsets.get(i));
        outputStream.writeLong(segPMSOffsets.get(i));
        outputStream.writeLong(segLengths.get(i));
        if (withPageMeta) {
          for (int j = 0; j < numClusters; j++) {
            // segMetasList.get(i)[j].write(outputStream);
            PageMeta pageMeta = segMetasList.get(i)[j];
            pageMeta.write(outputStream);
          }
        }
      }

      outputStream.writeInt(numClusters);
      outputStream.writeInt(curSegIdx);
      outputStream.writeLong(segIdxOffset);

      LOG.info("Finished @ position " + outputStream.getPos());
      // if (this.closeOutputStream) {
      // LOG.info("fs.size"+fs.getLength(path));
      // LOG.info("fs.name"+fs.getName());
      // LOG.info("fs.backup"+fs.getReplication(path));
      // LOG.info("fs.status"+fs.getFileStatus(path));
      // // LOG.info(""+fs.setVerifyChecksum(verifyChecksum););
      // fs.setVerifyChecksum(true);
      // fs.printStatistics();
      outputStream.close();
      // LOG.info("fs.size"+fs.getLength(path));
      // LOG.info("fs.name"+fs.getName());
      // LOG.info("fs.backup"+fs.getReplication(path));
      // LOG.info("fs.status"+fs.getFileStatus(path));
      outputStream = null;
      // DistributedFileSystem dfs=(DistributedFileSystem)fs ;
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      fs.rename(tmpPath, path);
      // fs.create(path);
      // dfs.moveToLocalFile(tmpPath,finalOutPath);
      fs.delete(tmpPath, true);
      // fs.delete(tmpoutputPath, true);

      // fs.MoveTask
    }

    @Override
    public synchronized void close() throws IOException {
      tabConfig.close();
      fileClose();

    }
  }

  public static class SegmentIndexRef {
    public int numSegs;
    public int numClusters;
    public long[] segOffsets;
    public long[] segPMSOffsets;
    public long[] segLengths;
    public PageMeta[][] segMetas;
  }

  /**
   * Segment Index Reader. (read in the segment index for M/R splitting.)
   */
  public static class SegmentIndexReader implements Closeable {

    long segIndexOffset;

    SegmentIndexRef ref = new SegmentIndexRef();

    // Stream to read from
    private FSDataInputStream istream = null;
    private final long fileSize;

    private final boolean withPageMeta;

    public SegmentIndexReader(FileSystem fs, Path path) throws IOException {
      this(fs, path, true);
    }

    public SegmentIndexReader(FileSystem fs, Path path, boolean withPM)
        throws IOException {
      istream = fs.open(path);
      fileSize = fs.getFileStatus(path).getLen();
      this.withPageMeta = withPM;
      LOG.info("Open Segment File " + path + " : file length is " + fileSize
          + " , with page meta : " + withPageMeta);
    }

    public synchronized void readSegIndex() throws IOException {
      istream.seek(fileSize - 2 * Bytes.SIZEOF_INT - Bytes.SIZEOF_LONG);
      ref.numClusters = istream.readInt();
      ref.numSegs = istream.readInt();
      segIndexOffset = istream.readLong();

      LOG.info("Trying to read " + ref.numSegs + " segments at position " + segIndexOffset);
      ref.segOffsets = new long[ref.numSegs];
      ref.segPMSOffsets = new long[ref.numSegs];
      ref.segLengths = new long[ref.numSegs];
      if (withPageMeta) {
        ref.segMetas = new PageMeta[ref.numSegs][];
      }

      istream.seek(segIndexOffset);

      for (int i = 0; i < ref.numSegs; i++) {
        ref.segOffsets[i] = istream.readLong();
        ref.segPMSOffsets[i] = istream.readLong();
        ref.segLengths[i] = istream.readLong();
        if (withPageMeta) {
          ref.segMetas[i] = new PageMeta[ref.numClusters];
          for (int j = 0; j < ref.numClusters; j++) {
            ref.segMetas[i][j] = new PageMeta();
            ref.segMetas[i][j].readFields(istream);
          }
        }
      }
    }

    public synchronized long[] getSegOffsets() {
      return ref.segOffsets;
    }

    public synchronized long[] getSegPMSOffsets() {
      return ref.segPMSOffsets;
    }

    public synchronized long[] getSegLengths() {
      return ref.segLengths;
    }

    public synchronized PageMeta[][] getSegMetas() {
      return ref.segMetas;
    }

    public synchronized int getNumSegs() {
      return ref.numSegs;
    }

    public synchronized SegmentIndexRef getRef() {
      return ref;
    }

    @Override
    public synchronized void close() throws IOException {
      if (istream == null) {
        return;
      }

      istream.close();
      istream = null;
    }

  }

  /**
   * Counters to calculate read pages
   */
  public static enum SegmentReadPageCounter {
    CLUSTER1, CLUSTER2, CLUSTER3, CLUSTER4, CLUSTER5, CLUSTER6, CLUSTER7,
    CLUSTER8, CLUSTER9, CLUSTER10, CLUSTER11, CLUSTER12, CLUSTER13,
    CLUSTER14, CLUSTER15, CLUSTER16, OTHERCLUSTERS
  }

  /**
   * Counters to calculate skipped pages
   */
  public static enum SegmentSkippedPageCounter {
    CLUSTER1, CLUSTER2, CLUSTER3, CLUSTER4, CLUSTER5, CLUSTER6, CLUSTER7,
    CLUSTER8, CLUSTER9, CLUSTER10, CLUSTER11, CLUSTER12, CLUSTER13,
    CLUSTER14, CLUSTER15, CLUSTER16, OTHERCLUSTERS
  }

  /**
   * Counters to calculate the cache hits
   */
  public static enum SegmentCacheHitCounter {
    CLUSTER1, CLUSTER2, CLUSTER3, CLUSTER4, CLUSTER5, CLUSTER6, CLUSTER7,
    CLUSTER8, CLUSTER9, CLUSTER10, CLUSTER11, CLUSTER12, CLUSTER13,
    CLUSTER14, CLUSTER15, CLUSTER16, OTHERCLUSTERS
  }

  /** Counters to calculate the position seek times */
  public static enum SegmentPosSeekCounter {
    MOVEON, MOVEBACK
  }

  /**
   * <p>
   * SegmentReader is used to read a segment. It wasn't used to read the actual data. A
   * <i>SegmentReader</i> is used to process the segment page meta section and generated the related
   * <i> ClusterReader</i>s to read the actual data.
   * </p>
   *
   */
  public static class SegmentReader implements Closeable {

    public static enum READMODE {
      /**
       * During point query, we use a shared global lru page cache
       * to reduce the overhead of random access.
       */
      POINTQUERY,
      /**
       * During m/r scan query, we use a simple queue-based page cache
       * to reduce the overhead of random access in a segment split.
       */
      MR
    }

    //
    Configuration conf;

    // stream to read in
    private final FSDataInputStream istream;

    // read in the page meta section
    PageMetaSection pms = null;

    // Segment information
    long segmentOffset;
    long segmentLength;
    long segmentPMSOffset;

    // Number of Clusters
    int numClusters;
    long[] clusterOffsets;

    // Cache Pool
    // pcp just used in m/r mode
    PageCache[] pcp;
    // pagecache used in POINTQUERY mode
    BlockCache pagecache;

    // Statistics of Cache Pool
    int pageLoads;
    int cacheHits;

    Map<Integer, ScanMode[]> scanMap = null;

    Reporter reporter = null;

    boolean withPageMeta;

    READMODE mode;
    int segId;

    /**
     * Create the Segment Reader.
     *
     * @param fs
     *          which file system that store the segment file
     * @param file
     *          the path of the segment file
     * @param buffersize
     *          the length of the buffer size to read the data
     * @param segmentOffset
     *          the file offset of the segment in the segment file
     * @param segmentLength
     *          the length of the segment
     * @param segmentPMSOffset
     *          the file offset of the page meta section of the segment
     * @throws IOException
     */
    public SegmentReader(Configuration conf, FileSystem fs, Path file, int segId, int buffersize,
        long segmentOffset, long segmentLength, long segmentPMSOffset) throws IOException {
      this(conf, fs, file, segId, buffersize, segmentOffset, segmentLength, segmentPMSOffset, true,
          READMODE.MR);
    }

    public SegmentReader(Configuration conf, FileSystem fs, Path file, int segId, int buffersize,
        long segmentOffset, long segmentLength, long segmentPMSOffset,
        boolean withPageMeta) throws IOException {
      this(conf, fs, file, segId, buffersize, segmentOffset, segmentLength, segmentPMSOffset,
          withPageMeta,
          READMODE.MR);
    }

    /**
     * Create the Segment Reader
     *
     * @param fs
     *          which file system that store the segment file
     * @param file
     *          the path of the segment file
     * @param buffersize
     *          the length of the buffer size to read the data
     * @param segmentOffset
     *          the file offset of the segment in the segment file
     * @param segmentLength
     *          the length of the segment
     * @param segmentPMSOffset
     *          the file offset of the page meta section of the segment
     * @param withPageMeta
     *          do we need to read the max/min page meta
     * @throws IOException
     */
    public SegmentReader(Configuration conf, FileSystem fs, Path file, int segId, int buffersize,
        long segmentOffset, long segmentLength, long segmentPMSOffset,
        boolean withPageMeta, READMODE readMode) throws IOException {
      this.conf = conf;
      this.istream = fs.open(file, buffersize);

      this.segmentOffset = segmentOffset;
      this.segmentLength = segmentLength;
      this.segmentPMSOffset = segmentPMSOffset;
      this.withPageMeta = withPageMeta;

      this.mode = readMode;
      this.segId = segId;
    }

    /**
     * Init a M/R repoter, so we can collect the statistics of the activities
     * of a segment reader during query processing
     *
     * @param reporter
     *          the M/R reporter
     */
    public synchronized void initReporter(Reporter reporter) {
      this.reporter = reporter;
    }

    /**
     * Load the page meta section.
     * Note: this methods need to be called before any other activity
     *
     * @throws IOException
     */
    public synchronized void loadPMS() throws IOException {
      // move to the offset of page meta section
      istream.seek(segmentPMSOffset);
      pms = new PageMetaSection(withPageMeta);
      pms.readFields(istream);

      // we get the page meta section, we know the number of clusters
      numClusters = pms.getPageMetaLists().length;

      LOG.info("Load page meta section with " + numClusters + " clusters.");

      if (mode == READMODE.MR) {
        pcp = new PageCache[numClusters];
        for (int i = 0; i < numClusters; i++) {
          pcp[i] = new SimplePageCache();
        }
      } else if (mode == READMODE.POINTQUERY) {
        pagecache = getBlockCache(conf);
      }

      // read in the cluster offsets
      clusterOffsets = new long[numClusters];
      for (int i = 0; i < numClusters; i++) {
        clusterOffsets[i] = istream.readLong();
      }
    }

    public synchronized void buildScanMap(ExprDesc expr, ClusterAccessor[] accessors) {
      if (pms == null) {
        return;
      }
      pms.setClusterAccessors(accessors);

      this.scanMap = pms.computeScanMap(expr);
    }

    /**
     * Clear the scan map of the last query. So the segment reader can be re-used
     * in the following query processing.
     */
    public synchronized void clearScanMap() {
      scanMap = null;
    }

    /**
     * Create a cluster reader to read the actual data.
     *
     * @param clusterId
     *          which cluster to read data
     * @param cachePage
     *          do we need to cache the page in the buffer
     * @return cluster reader
     */
    public synchronized ClusterReader newClusterReader(int clusterId, boolean cachePage) {
      long clusterLength;
      if (clusterId == numClusters - 1) {
        clusterLength = segmentPMSOffset - clusterOffsets[clusterId];
      } else {
        clusterLength = clusterOffsets[clusterId + 1] - clusterOffsets[clusterId];
      }
      ScanMode[] modes = null;
      if (scanMap != null) {
        modes = scanMap.get(clusterId);
      }
      return new ClusterReader(this, clusterId, clusterOffsets[clusterId],
          clusterLength, pms.getPageMetaLists()[clusterId], modes, cachePage);
    }

    /**
     * Read a page of clusterId at position
     *
     * @param clusterId
     * @param position
     * @return
     */

    //    DynamicByteArray  dynamicBuffer = new DynamicByteArray();
    //    dynamicBuffer.add(bytes, 0, bytes.length);
    //
    //    ByteBuffer inBuf = ByteBuffer.allocate(dynamicBuffer.size());
    //    //  System.out.println("56  "+inBuf.getInt());
    //    dynamicBuffer.setByteBuffer(inBuf, 0, dynamicBuffer.size());
    //
    //    inBuf.flip();
    //
    //    RunLengthIntegerReader in = new RunLengthIntegerReader(InStream.create
    //        ("test", inBuf, codec, (int)readfile.length()), true);
    //    //  int  count=0 ;
    //    int[]  result=new  int[fileLong] ;
    //    for(int i=0; i < fileLong; ++i) {
    //
    //      result[i]= (int) in.next();
    //      count ++ ;
    //    }
    ////    inBuf.clear();
    public synchronized ByteBuffer readPage(int clusterId, int position, boolean cachePage)
        throws IOException {
      PageMetaList pmList = pms.getPageMetaLists()[clusterId];
      int pageId = Utils.findTargetPos(pmList.getMetaList(), 0, pmList.getMetaList().size() - 1,
          position);
      if (pageId < 0) {
        throw new IOException("No page in segment " + this.segId + " contains position " + position);
      }
      long offset = pmList.getOffsetList().get(pageId);
      long length;
      if (pageId == pmList.getOffsetList().size() - 1) {
        long clusterLength;
        if (clusterId == numClusters - 1) {
          clusterLength = segmentPMSOffset - clusterOffsets[clusterId];
        } else {
          clusterLength = clusterOffsets[clusterId + 1] - clusterOffsets[clusterId];
        }
        length = clusterOffsets[clusterId] + clusterLength - offset;
      } else {
        length = pmList.getOffsetList().get(pageId + 1) - offset;
      }
      return readPage(clusterId, pageId, offset, length, cachePage);
    }

    /**
     * Read in a file page.
     *
     * @param clusterId
     *          which cluster to read
     * @param pageId
     *          which page to read
     * @param cachePage
     *          need to cache a page?
     * @return Block wrapped in a ByteBuffer.
     * @throws IOException
     */
    synchronized ByteBuffer readPage(int clusterId, int pageId, long offset, long length,
        boolean cachePage)
            throws IOException {
      pageLoads++;
      ByteBuffer cachedPage = null;
      if (mode == READMODE.MR) {
        cachedPage = pcp[clusterId].getPage(pageId);
      } else if (mode == READMODE.POINTQUERY) {
        cachedPage = pagecache.getBlock(makePageName(segId, clusterId, pageId));
      }
      if (cachedPage != null) {
        cacheHits++;
        if (reporter != null) {
          if (clusterId < 16) {
            reporter.incrCounter(SegmentCacheHitCounter.values()[clusterId], 1);
          } else {
            reporter.incrCounter(SegmentCacheHitCounter.OTHERCLUSTERS, 1);
          }
        }
        return cachedPage.duplicate();
      }

      /**
       * Report the related activities to M/R frameworks, so we will know
       * what happened during query processing
       */
      if (reporter != null) {
        if (clusterId < 16) {
          reporter.incrCounter(SegmentReadPageCounter.values()[clusterId], 1);
        } else {
          reporter.incrCounter(SegmentReadPageCounter.OTHERCLUSTERS, 1);
        }

        if (istream.getPos() - offset > 0) {
          reporter.incrCounter(SegmentPosSeekCounter.MOVEBACK, 1);
        } else {
          reporter.incrCounter(SegmentPosSeekCounter.MOVEON, 1);
        }
      }

      // ByteBuffer buf = ByteBuffer.allocate(longToInt(length));
      // istream.readFully(offset, buf.array());

      // Read the page from filesystem
      InputStream is = new BoundedRangeFileInputStream(istream, offset, length);
      ByteBuffer buf = ByteBuffer.allocate(longToInt(length));
      IOUtils.readFully(is, buf.array(), 0, buf.capacity());
      is.close();

      if (cachePage) {
        if (mode == READMODE.MR) {
          pcp[clusterId].cachePage(pageId, buf.duplicate());
        } else if (mode == READMODE.POINTQUERY) {
          pagecache.cacheBlock(makePageName(segId, clusterId, pageId), buf.duplicate(), true);
        }
      }
      return buf;
    }


    @Override
    public synchronized void close() throws IOException {
      if (istream != null) {
        istream.close();
      }
    }

  }

  /**
   * A <i>ClusterReader</i> is used to handle the read action
   * of a specified cluster.
   */
  public static class ClusterReader {

    private final SegmentReader sr;

    private final int clusterId;
    private final long clusterOffset;
    private final long clusterLength;
    private final List<PageMeta> pmList;
    private final List<Long> poList;
    private final ScanMode[] scanmode;
    private boolean cachePage;

    int curPageId;
    int numPages;

    PosRLEChunk prb = new PosRLEChunk();

    Reporter reporter = null;

    /**
     * ClusterReader Constructor
     *
     * @param sr
     * @param clusterId
     *          which cluster to read
     * @param clusterOffset
     *          the file offset of the cluster
     * @param clusterLength
     *          the length of the cluster
     * @param pml
     *          the pagemeta list of the cluster
     * @param sm
     *          the scan mode of the query
     * @param cachePage
     *          do we need to cache a page for random access
     */
    public ClusterReader(SegmentReader sr, int clusterId,
        long clusterOffset, long clusterLength, PageMetaList pml,
        ScanMode[] sm, boolean cachePage) {
      // System.out.println("Open reader for cluster " + clusterId + " offset " + clusterOffset +
      // " length " + clusterLength);
      this.sr = sr;
      this.clusterId = clusterId;
      this.clusterOffset = clusterOffset;
      this.clusterLength = clusterLength;
      this.pmList = pml.getMetaList();
      this.poList = pml.getOffsetList();
      this.cachePage = cachePage;
      scanmode = sm;
      numPages = poList.size();

      curPageId = 0;
    }

    public synchronized void setCachePage(boolean cachePage_) {
      this.cachePage = cachePage_;
    }

    public synchronized void initReporter(Reporter reporter) {
      this.reporter = reporter;
    }

    public synchronized int getNumPages() {
      return numPages;
    }

    public synchronized int getCurPageId() {
      return curPageId;
    }

    public synchronized boolean isPageCached() {
      return cachePage;
    }

    /**
     * Read a next page (sequencely)
     *
     * @return the next page data
     * @throws IOException
     */
    public synchronized byte[] nextPage() throws IOException {
      if (curPageId < numPages) {
        long offset = poList.get(curPageId);
        long length = curPageId == numPages - 1 ? clusterOffset + clusterLength - offset :
          poList.get(curPageId + 1) - offset;
        // System.out.println("reade Page " + curPageId + " offset " + offset + " length " +
        // length);

        //  LOG.info("1567   byte[] nextPage()  length  "+length+"   clusterId  "+clusterId+"  curPageId  "+curPageId+"  offset  "+offset);
        //        System.out.println("1567   byte[] nextPage()  length  "+length+"   clusterId  "+clusterId+"  curPageId  "+curPageId+"  offset  "+offset);
        byte[] page = sr.readPage(clusterId, curPageId, offset, length, cachePage).array();
        //   LOG.info("1567   byte[] nextPage()  pageLength   "+page.length);
        //   System.out.println("1567   byte[] nextPage()  pageLength   "+page.length);
        curPageId++;
        return page;
      } else {
        return null;
      }
    }

    /**
     * @deprecated
     * @param predicate
     * @return the page data
     * @throws IOException
     */
    @Deprecated
    public synchronized byte[] nextPage(Predicate predicate) throws IOException {
      return null;
    }

    /**
     * Skip to the page thats contain the target position.
     * It is useful during position filtering.
     *
     * @param pos
     *          position
     * @return the page data
     * @throws IOException
     */
    public synchronized byte[] skipToPosAndGetPage(int pos) throws IOException {
      // System.err.println("[SegmentFile]skipToPosAndGetPage : skip to pos " + pos +
      // " to read a page.");

      if (curPageId >= numPages) {
        return null;
      }

      int skipToIdx = Utils.findTargetPos(pmList, curPageId, numPages - 1, pos);
      if (skipToIdx >= 0) {
        if (reporter != null && skipToIdx > curPageId) {
          if (clusterId < 16) {
            reporter.incrCounter(SegmentSkippedPageCounter.values()[clusterId], skipToIdx
                - curPageId - 1);
          } else {
            reporter
            .incrCounter(SegmentSkippedPageCounter.OTHERCLUSTERS, skipToIdx - curPageId - 1);
          }
        }
        curPageId = skipToIdx;
      }
      return nextPage();
    }

    /**
     * Get the last position of the cluster
     *
     * @return last position
     */
    public synchronized int getLastPos() {
      PageMeta pm = pmList.get(numPages - 1);
      return pm.startPos + pm.numPairs - 1;
    }

    /**
     * <p>
     * Read in the next necessary page by predicate. (skip all the negative pages) <br>
     * 1) if the page is a rough page, return the rough page and its scan mode. <br>
     * 2) if the page is a positive page, return the first positive page and its scan mode, and we
     * also collect the continuous position range until we encounters a rough/negative page.
     * </p>
     *
     * <p>
     * this method is used in producing position chunks stream</i>
     *
     * @param modes
     *          the scan mode of the page(output)
     * @param blks
     *          the position range of the predicate pages(output)
     * @return the reference to the page
     * @throws IOException
     */
    public synchronized byte[] nextPredicatePagePos(ScanMode[] modes, PosChunk[] blks)
        throws IOException {
      if (scanmode == null) {
        modes[0] = ScanMode.Rough;
        if (curPageId < numPages) {
          PageMeta tmpPm = pmList.get(curPageId);
          prb.setTriple(null, tmpPm.startPos, tmpPm.numPairs);
          blks[0] = prb;
          return nextPage();
        } else {
          return null;
        }
      }

      byte[] page = null;

      // skip all the negative pages
      int prevPageId = curPageId;
      while (curPageId < numPages && scanmode[curPageId] == ScanMode.Negative) {
        curPageId++;
      }
      if (reporter != null) {
        if (clusterId < 16) {
          reporter.incrCounter(SegmentSkippedPageCounter.values()[clusterId], curPageId
              - prevPageId - 1);
        } else {
          reporter.incrCounter(SegmentSkippedPageCounter.OTHERCLUSTERS, curPageId - prevPageId - 1);
        }
      }

      if (curPageId < numPages) {
        if (scanmode[curPageId] == ScanMode.Rough) {
          modes[0] = ScanMode.Rough;
          PageMeta pm = pmList.get(curPageId);
          // System.err.println("Read page " + curPageId + " : start from " + pm.startPos +
          // ", numReps " + pm.numPairs);
          prb.setTriple(null, pm.startPos, pm.numPairs);
          blks[0] = prb;
          page = nextPage();
        } else if (scanmode[curPageId] == ScanMode.Positive) {
          modes[0] = ScanMode.Positive;

          PageMeta pm = pmList.get(curPageId);
          int startPos = pm.startPos;
          int numPairs = pm.numPairs;
          // System.err.println("Read page " + curPageId + " : start from " + startPos + ", numReps"
          // + numPairs);
          page = nextPage();
          while (curPageId < numPages &&
              scanmode[curPageId] == ScanMode.Positive) {
            pm = pmList.get(curPageId);
            numPairs += pm.numPairs;
            // System.err.println("Read page " + curPageId + " : start from " + pm.startPos +
            // ", numReps " + pm.numPairs);
            curPageId++;
          }
          // System.err.println("Return scan range : start from " + startPos + ", numReps " +
          // numPairs + ".");
          prb.setTriple(null, startPos, numPairs);
          blks[0] = prb;
        } else {
          assert (false);
        }
      }

      return page;
    }

    /**
     * Read in the next page by predicates
     *
     * @param modes
     *          the scan mode of the next page
     * @return the next page data
     * @throws IOException
     */
    public synchronized byte[] nextPredicatePageValue(ScanMode[] modes) throws IOException {
      if (scanmode == null) {
        modes[0] = ScanMode.Rough;
        return nextPage();
      }

      byte[] page = null;

      int prevPageId = curPageId;
      // skip all the negative pages
      while (curPageId < numPages && scanmode[curPageId] == ScanMode.Negative) {
        curPageId++;
      }
      if (reporter != null) {
        if (clusterId < 16) {
          reporter.incrCounter(SegmentSkippedPageCounter.values()[clusterId], curPageId
              - prevPageId - 1);
        } else {
          reporter.incrCounter(SegmentSkippedPageCounter.OTHERCLUSTERS, curPageId - prevPageId - 1);
        }
      }

      if (curPageId < numPages) {
        modes[0] = scanmode[curPageId];
        page = nextPage();
      }
      return page;
    }

  }

  // Utility methods.
  /*
   * @param l Long to convert to an int.
   *
   * @return <code>l</code> cast as an int.
   */
  static synchronized int longToInt(final long l) {
    // Expecting the size() of a block not exceeding 4GB. Assuming the
    // size() will wrap to negative integer if it exceeds 2GB (From tfile).
    return (int) (l & 0x00000000ffffffffL);
  }

  /**
   * Return the global block cache
   *
   * @param conf
   *          The current configuration
   * @return the block cache or null
   */
  public static synchronized BlockCache getBlockCache(Configuration conf) {
    if (globalPageCache != null) {
      return globalPageCache;
    }

    float cachePercentage = conf.getFloat(SEGFILE_CACHE_SIZE_KEY, 0.0f);
    if (cachePercentage == 0L) {
      return null;
    }
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(SEGFILE_CACHE_SIZE_KEY +
          " must be between 0.0 and 1.0, not > 1.0");
    }

    // Calculate the amount of heap to give the heap
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long cacheSize = (long) (mu.getMax() * cachePercentage);
    LOG.info("Allocating LruPageCache with maximum size " +
        StringUtils.humanReadableInt(cacheSize));
    globalPageCache = new LruBlockCache(cacheSize,
        MastiffMapReduce.getTablePageSize(conf));

    return globalPageCache;
  }

  static synchronized String makePageName(int segId, int clusterId, int pageId) {
    StringBuilder sb = new StringBuilder();
    sb.append(segId).append('-').append(clusterId).append('-').append(pageId);
    return sb.toString();
  }
}
