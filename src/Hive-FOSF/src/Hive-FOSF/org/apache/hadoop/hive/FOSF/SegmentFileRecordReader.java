package org.apache.hadoop.hive.mastiff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.MTableDesc;
import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.ValidColumnsInCF;
import org.apache.hadoop.hive.mastiff.SegmentFile.ClusterReader;
import org.apache.hadoop.hive.mastiff.SegmentFile.SegmentReader;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import cn.ac.ncic.mastiff.Chunk;
import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.io.coding.Decoder;
import cn.ac.ncic.mastiff.io.coding.EnDecode;
import cn.ac.ncic.mastiff.io.segmentfile.PageMeta.ScanMode;
import cn.ac.ncic.mastiff.mapred.MastiffMapReduce;

/**
 *
 * SegmentFileRecordReader generate record from Segment File<br/>
 * In order to minimize the quantity of data to be read from file System,
 * we use filter passed from Hive PushFilterDown in two phases:<ol><li>
 * Use PageMeta in building ScanMap and get possible line range</li><li>
 * At record level, we use filter to judge if current line right and
 * only pass the right line into hive's optree
 *
 */
public class SegmentFileRecordReader implements RecordReader<NullWritable, RowWritable> {

  public static final Log LOG = LogFactory.getLog(SegmentFileRecordReader.class);

  private final SegmentFileSplit sfs;

  private final JobConf jobConf;
  private final FileSystem fs;

  private final Path segPath;
  public  SegmentReader sr;
  private final Reporter reporter;
  private final int ioBufSize;
  private final boolean withPageMeta;
  private final int segmentId;
  private HashMap<Integer, ClusterReader> crs;
  private  HashMap<Integer, Decoder> decoders;
  private final HashMap<Integer, Chunk> curChunks;

  private final HashMap<Integer, ValidColumnsInCF> cfWithCol;
  private final List<Integer> validCfs;
  private List<Integer> filterCfs;
  private List<Integer> filterColumns;
  private  List<Integer> rgs; //row group ends
  private  ScanMode[] resultSMs; //scanmodes for row group

  private int curRGIdx = 0;
  private int curPos = 0;

  private final HashMap<Integer, Cell> cells;

  private int curSegIdx;
  private long curLine = 0;
  private final long segOffset;
  private final long segLength;
  private final  long segPMSOffset;
  private long segLineNum;

  private final MTableDesc tblDesc;
  private ExprNodeDesc filterDesc = null;
  private final List<Integer> readColIDs;
  private boolean isFirst = true;




  public SegmentFileRecordReader(InputSplit split, JobConf jobConf, Reporter reporter
      ) throws IOException {

    sfs = (SegmentFileSplit) split;
    this.jobConf = jobConf;
    fs = FileSystem.get(jobConf);
    this.reporter = reporter;
    segPath = sfs.getPath().makeQualified(fs);
    segOffset = sfs.getSegOffset();
    segLength = sfs.getLength();
    segPMSOffset = sfs.getSegPmsOffset();
    segmentId = sfs.getSegId();
    ioBufSize = MastiffMapReduce.getSegmentIOBuffer(jobConf);
    withPageMeta = MastiffMapReduce.getReadMastiffPageMeta(jobConf);

    //get tblDesc & filter from jobconf
    String filterExprSerialized = jobConf.get(SegmentFileInputFormat.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized != null) {
      filterDesc = Utilities.deserializeExpression(filterExprSerialized, jobConf);
    }

    String tblName = sfs.getTableName();
    try {
      MastiffHandlerUtil.setCFMeta(jobConf, tblName);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    tblDesc = MastiffHandlerUtil.getMTableDesc(jobConf);
    MastiffHandlerUtil.getCFTypes(tblDesc);

    readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);
    cfWithCol = MastiffHandlerUtil.getCfValidColumns(tblDesc, readColIDs);
    validCfs = new ArrayList<Integer>();
    for(Integer key : cfWithCol.keySet()) {
      validCfs.add(key);
    }

    if(filterDesc != null) {
      filterCfs = new ArrayList<Integer>();
      filterColumns = new ArrayList<Integer>();
      MastiffHandlerUtil.getFilterInfo(tblDesc, filterDesc, filterCfs, filterColumns);
    }

    cells = new HashMap<Integer,Cell>();
    curChunks = new HashMap<Integer, Chunk>();
    for(Integer cfIdx : validCfs) {
      Cell curcell = new Cell();
      curcell.init(Arrays.asList(tblDesc.clusterTypes[cfIdx]));
      cells.put(cfIdx, curcell);
    }

    initSR();
    initCR();
    getDecoders();

  }

  public void initSR() throws IOException {
    sr = new SegmentReader(jobConf, fs, segPath, segmentId, ioBufSize,
        segOffset, segLength, segPMSOffset, withPageMeta);
    sr.initReporter(reporter);
    sr.loadPMS();
    if(filterDesc != null) {
      rgs= MastiffHandlerUtil.getFittestSplit(filterCfs, sr.pms);
      resultSMs = MastiffHandlerUtil.getScanModesForRG(tblDesc, filterDesc, sr.pms, rgs);
      LOG.info(Arrays.toString(resultSMs));
      System.out.println(Arrays.toString(resultSMs));
    }
    segLineNum = MastiffHandlerUtil.getSegmentLineNum(sr.pms);
    // if(filter != null) {
    // buildScanMap()
    // }
  }

  public void initCR() {
    crs = new HashMap<Integer, ClusterReader>();
    for (Integer clusterId : validCfs) {
      crs.put(clusterId, sr.newClusterReader(clusterId, false));
    }
  }

  public void getDecoders() {
    decoders = new HashMap<Integer, Decoder>();
    for (Integer cfId : crs.keySet()) {
      Decoder decompressor =
          EnDecode.getDecoder(-1, cells.get(cfId).getFixedLen(),
              tblDesc.clusterAlgos[cfId], tblDesc.clusterCodingTypes[cfId]);
      decoders.put(cfId, decompressor);
    }
  }


  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public NullWritable createKey() {
    // TODO Auto-generated method stub
    return NullWritable.get();
  }

  @Override
  public RowWritable createValue() {
    // TODO Auto-generated method stub
    return new RowWritable();
  }

  @Override
  public long getPos() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean next(NullWritable key, RowWritable value) throws IOException {
    if(curLine >= segLineNum) {
      return false;
    }
    curLine++;
    if(filterDesc == null) {
      if(updateCurrentChunks()) {
        List<ValPair> vps = new ArrayList<ValPair>();
        for (int cfIdx : validCfs) {
          vps.add(curChunks.get(cfIdx).next());
        }
        if(isFirst) {
          value.set((byte) 1, vps, validCfs, readColIDs);
          isFirst = false;
        } else {
          value.set((byte)0, vps, null, null);
        }
        return true;
      }
      return false;
    } else { // filterDesc != null
      int positivePos = getNextPositivePos();
      if(positivePos != -1) {
        List<ValPair> vps = getVpsAtPos(validCfs, positivePos);
        if(vps == null) {
          return false;
        }
        if(isFirst) {
          value.set((byte) 1, vps, validCfs, readColIDs);
          isFirst = false;
        } else {
          value.set((byte)0, vps, null, null);
        }
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Check and update curChunks in case we could always get a new row from it
   * Usage: in next(), before we actually get value from curChunks
   * @return ture if we can still get next row
   * @throws IOException
   */
  private boolean updateCurrentChunks() throws IOException {
    for(Integer cfIdx : validCfs) {
      Chunk curChunk = curChunks.get(cfIdx);
      Decoder curDecompressor = decoders.get(cfIdx);
      ClusterReader curCr = crs.get(cfIdx);
      while(curChunk == null || !curChunk.hasNext()) {
        curChunk = curDecompressor.nextChunk();
        if(curChunk == null) {
          while(!curDecompressor.hashNextChunk()) {
            byte[] page = curCr.nextPage();
            if(page == null) {
              return false;
            }
            curDecompressor.reset(page, 0, page.length);
            curChunk = curDecompressor.nextChunk();
            if(curChunk != null) {
              break;
            }
          }
        }
      }
      curChunks.put(cfIdx, curChunk);
    } //end for
    return true;
  }

  private List<ValPair> getVpsAtPos(List<Integer> cfs ,int pos) throws IOException {
    for(Integer curCf : cfs) {
      Chunk curChunk = curChunks.get(curCf);
      Decoder curDecompressor = decoders.get(curCf);
      ClusterReader curCr = crs.get(curCf);
      while(curChunk == null || !posInChunk(pos, curChunk)) {
        if(curDecompressor.getBuffer() == null ||
            pos < curDecompressor.beginPos() || pos > curDecompressor.endPos()) {
          byte[] page = curCr.skipToPosAndGetPage(pos);
          if(page == null) {
            return null;
          }
          curDecompressor.reset(page, 0, page.length);
        }
        curChunk = curDecompressor.getChunkByPosition(pos);
      }
      curChunks.put(curCf, curChunk);
    } //end for

    List<ValPair> curRecord = new ArrayList<ValPair>();
    for(Integer curCf: cfs) {
      Chunk curChunk = curChunks.get(curCf);
      int idxInCurChunk = pos - curChunk.beginPos();
      curRecord.add(curChunk.getPairByIdx(idxInCurChunk));
    }
    return curRecord;

  }

  private int getNextPositivePos() throws IOException {
    while(curRGIdx < rgs.size()) {
      int curRGEnd = rgs.get(curRGIdx);
      if(curPos > curRGEnd) {
        curRGIdx++;
        continue;
      }
      ScanMode cursm = resultSMs[curRGIdx];
      if(cursm == ScanMode.Positive) {
        return curPos++;
      } else if (cursm == ScanMode.Negative) {
        if(curRGIdx == rgs.size() - 1) {
          return -1;
        }
        else {
          curPos = rgs.get(curRGIdx) + 1;
          curRGIdx++;
          continue;
        }
      } else { //cursm == ScanMode.Rough
        List<ValPair> curRecord = getVpsAtPos(filterCfs, curPos);
        if(curRecord == null) {
          return -1;
        }
        Boolean res = MastiffHandlerUtil.evaluate(tblDesc, filterDesc, filterCfs, filterColumns, curRecord);
        if(Boolean.TRUE.equals(res)) {
          return curPos++;
        } else {
          curPos++;
        }
      } //end else
    }//end while
    return -1;
  }

  private boolean posInChunk(int pos, Chunk curChunk) {
    int start = curChunk.beginPos();
    int end = curChunk.endPos();
    if (pos < start || pos > end) {
      return false;
    }
    return true;
  }

}
