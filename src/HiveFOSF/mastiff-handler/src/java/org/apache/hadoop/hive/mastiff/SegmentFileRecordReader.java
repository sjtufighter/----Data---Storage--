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
 * we use filter passed from Hive PushFilterDown in two phases:
 * <ol>
 * <li>
 * Use PageMeta in building ScanMap and get possible line range</li>
 * <li>
 * At record level, we use filter to judge if current line right and only pass the right line into
 * hive's optree
 *
 */
public class SegmentFileRecordReader implements RecordReader<NullWritable, RowWritable> {

  public static final Log LOG = LogFactory.getLog(SegmentFileRecordReader.class);
  public static final byte ZERO = 0;
  public static final byte ONE = 1;

  SegmentFileSplit curSfs;
  SegmentFileCombineSplit sfcs;
  List<SegmentFileSplit> sfsLst;

  JobConf jobConf;
  FileSystem fs;

  Path segPath;
  SegmentReader curSr;
  Reporter reporter;
  int ioBufSize;
  boolean withPageMeta;
  int segmentId;
  HashMap<Integer, ClusterReader> crs;
  HashMap<Integer, Decoder> decoders;
  HashMap<Integer, Chunk> curChunks;

  List<ValPair> curVps;

  HashMap<Integer, ValidColumnsInCF> cfWithCol;
  List<Integer> validCfs;
  List<Integer> filterCfs;
  List<Integer> filterColumns;
  List<Integer> rgs; // row group ends
  ScanMode[] resultSMs; // scanmodes for row group

  RowEvaluator cachedEvaluator;

  int curRGIdx = 0;
  int curPos = 0;

  HashMap<Integer, Cell> cells;

  int curSegIdx = 0;
  int totSegNum;

  long curLine = 0;
  // line num in SegmentFileCombineSplit
  long totLine = 0;
  long segOffset;
  long segLength;
  long segPMSOffset;
  long segLineNum;

  private final MTableDesc tblDesc;
  private ExprNodeDesc filterDesc = null;
  private final List<Integer> readColIDs;
  private boolean isFirst = true;

  public void handleNextSegment() throws IOException {
    curSfs = sfsLst.get(curSegIdx);

    segPath = curSfs.getPath().makeQualified(fs);
    segOffset = curSfs.getSegOffset();
    segLength = curSfs.getLength();
    segPMSOffset = curSfs.getSegPmsOffset();
    segmentId = curSfs.getSegId();

    if (curSr != null) {
      curSr.close();
    }

    initSR();
    initCR();
    getDecoders();

    curRGIdx = 0;
    curPos = 0;
    curLine = 0;
    curSegIdx++;
  }

  public SegmentFileRecordReader(InputSplit split, JobConf jobConf, Reporter reporter)
      throws IOException {

    sfcs = (SegmentFileCombineSplit) split;
    sfsLst = sfcs.getParts();
    totSegNum = sfsLst.size();


    this.jobConf = jobConf;
    fs = FileSystem.get(jobConf);
    this.reporter = reporter;

    ioBufSize = MastiffMapReduce.getSegmentIOBuffer(jobConf);
    withPageMeta = MastiffMapReduce.getReadMastiffPageMeta(jobConf);

    // get tblDesc & filter from jobconf
    String filterExprSerialized = jobConf.get(SegmentFileInputFormat.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized != null && !filterExprSerialized.equals("")) {
      filterDesc = Utilities.deserializeExpression(filterExprSerialized, jobConf);
    }

    String tblName = sfcs.getTableName();
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
    for (Integer key : cfWithCol.keySet()) {
      validCfs.add(key);
    }

    if (filterDesc != null) {
      filterCfs = new ArrayList<Integer>();
      filterColumns = new ArrayList<Integer>();
      MastiffHandlerUtil.getFilterInfo(tblDesc, filterDesc, filterCfs, filterColumns);
      // since we remove filterOp from MapOperator's Optree, readColIds would not cotain
      // filter columns, so we just add them into validCfs
      for (Integer i : filterCfs) {
        if (!validCfs.contains(i)) {
          validCfs.add(i);
        }
      }
    }

    cells = new HashMap<Integer, Cell>();
    curChunks = new HashMap<Integer, Chunk>();
    for (Integer cfIdx : validCfs) {
      Cell curcell = new Cell();
      curcell.init(Arrays.asList(tblDesc.clusterTypes[cfIdx]));
      cells.put(cfIdx, curcell);
    }

    handleNextSegment();
  }

  public void initSR() throws IOException {
    curSr = new SegmentReader(jobConf, fs, segPath, segmentId, ioBufSize,
        segOffset, segLength, segPMSOffset, withPageMeta);
    curSr.initReporter(reporter);
    curSr.loadPMS();
    if (filterDesc != null) {
      rgs = MastiffHandlerUtil.getFittestSplit(filterCfs, curSr.pms);
      resultSMs = MastiffHandlerUtil.getScanModesForRG(tblDesc, filterDesc, curSr.pms, rgs);
      LOG.info(Arrays.toString(resultSMs));

      // Counter section, calculate rgs' scanMode & filtered line count

      reporter.incrCounter(FilterState.ALL, rgs.get(rgs.size() - 1) + 1);
      int p = 0;
      int n = 0;
      int r = 0;
      int passed = 0;
      int filtered = 0;
      List<Integer> rgLineNum = MastiffHandlerUtil.getRowsInRG(rgs);
      for (int i = 0; i < resultSMs.length; i++) {
        int cnt = rgLineNum.get(i);
        reporter.incrCounter(FilterState.GEN_ALL, cnt);
        switch (resultSMs[i]) {
        case Positive:
          p++;
          reporter.incrCounter(FilterState.POS_PASSED, cnt);
          break;
        case Negative:
          n++;
          reporter.incrCounter(FilterState.NEG_FILTERED, cnt);
          break;
        case Rough:
          r++;
          break;
        }
      }
      reporter.incrCounter(RG.POSITIVE, p);
      reporter.incrCounter(RG.NEGATIVE, n);
      reporter.incrCounter(RG.ROUGH, r);

    }
    segLineNum = MastiffHandlerUtil.getSegmentLineNum(curSr.pms);
  }

  public void initCR() {
    if (crs == null) {
      crs = new HashMap<Integer, ClusterReader>();
    } else {
      crs.clear();
    }
    for (Integer clusterId : validCfs) {
      crs.put(clusterId, curSr.newClusterReader(clusterId, false));
    }
  }

  public void getDecoders() {
    if (decoders == null) {
      decoders = new HashMap<Integer, Decoder>();
    } else {
      decoders.clear();
    }
    for (Integer cfId : crs.keySet()) {
      Decoder decompressor =
          EnDecode.getDecoder(-1, cells.get(cfId).getFixedLen(),
              tblDesc.clusterAlgos[cfId], tblDesc.clusterCodingTypes[cfId]);
      decoders.put(cfId, decompressor);
    }
  }


  @Override
  public void close() throws IOException {
    if (curSr != null) {
      curSr.close();
    }
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public RowWritable createValue() {
    return new RowWritable();
  }

  @Override
  public long getPos() throws IOException {
    return totLine;
  }

  @Override
  public float getProgress() throws IOException {
    return curSegIdx / totSegNum;
  }

  @Override
  public boolean next(NullWritable key, RowWritable value) throws IOException {
    boolean curSegFinished = false;
    while (true) {
      if (curLine >= segLineNum) {
        curSegFinished = true;
      }
      totLine++;
      curLine++;
      if (!curSegFinished) {
        if (filterDesc == null) {
          if (updateCurrentChunks()) {
            if (curVps == null) {
              curVps = new ArrayList<ValPair>();
            } else {
              curVps.clear();
            }
            for (int cfIdx : validCfs) {
              curVps.add(curChunks.get(cfIdx).next());
            }
            if (isFirst) {
              value.set(ONE, curVps, validCfs, readColIDs);
              isFirst = false;
            } else {
              value.set(ZERO, curVps, null, null);
            }
            return true;
          }
          curSegFinished = true;
        } else { // filterDesc != null
          int positivePos = getNextPositivePos();
          if (positivePos != -1) {
            List<ValPair> vps = getVpsAtPos(validCfs, positivePos);
            if (vps == null) {
              curSegFinished = true;
            }
            if (isFirst) {
              value.set(ONE, vps, validCfs, readColIDs);
              isFirst = false;
            } else {
              value.set(ZERO, vps, null, null);
            }
            return true;
          } else {
            curSegFinished = true;
          }
        } // single end
      }

      if (curSegFinished && (curSegIdx < totSegNum)) {
        handleNextSegment();
        curSegFinished = false;
      } else if (curSegFinished && (curSegIdx >= totSegNum)) {
        return false;
      }

    }
  }

  /**
   * Check and update curChunks in case we could always get a new row from it
   * Usage: in next(), before we actually get value from curChunks
   *
   * @return ture if we can still get next row
   * @throws IOException
   */
  private boolean updateCurrentChunks() throws IOException {
    for (Integer cfIdx : validCfs) {
      Chunk curChunk = curChunks.get(cfIdx);
      Decoder curDecompressor = decoders.get(cfIdx);
      ClusterReader curCr = crs.get(cfIdx);
      while (curChunk == null || !curChunk.hasNext()) {
        curChunk = curDecompressor.nextChunk();
        if (curChunk == null) {
          while (!curDecompressor.hashNextChunk()) {
            byte[] page = curCr.nextPage();
            if (page == null) {
              return false;
            }
            curDecompressor.reset(page, 0, page.length);
            curChunk = curDecompressor.nextChunk();
            if (curChunk != null) {
              break;
            }
          }
        }
      }
      curChunks.put(cfIdx, curChunk);
    } // end for
    return true;
  }

  private List<ValPair> getVpsAtPos(List<Integer> cfs, int pos) throws IOException {
    for (Integer curCf : cfs) {
      Chunk curChunk = curChunks.get(curCf);
      Decoder curDecompressor = decoders.get(curCf);
      ClusterReader curCr = crs.get(curCf);
      while (curChunk == null || !posInChunk(pos, curChunk)) {
        if (curDecompressor.getBuffer() == null ||
            pos < curDecompressor.beginPos() || pos > curDecompressor.endPos()) {
          byte[] page = curCr.skipToPosAndGetPage(pos);
          if (page == null) {
            return null;
          }
          curDecompressor.reset(page, 0, page.length);
        }
        curChunk = curDecompressor.getChunkByPosition(pos);
      }
      curChunks.put(curCf, curChunk);
    } // end for

    if (curVps == null) {
      curVps = new ArrayList<ValPair>();
    } else {
      curVps.clear();
    }
    for (Integer curCf : cfs) {
      Chunk curChunk = curChunks.get(curCf);
      int idxInCurChunk = pos - curChunk.beginPos();
      curVps.add(curChunk.getPairByIdx(idxInCurChunk));
    }
    return curVps;

  }

  private int getNextPositivePos() throws IOException {
    while (curRGIdx < rgs.size()) {
      int curRGEnd = rgs.get(curRGIdx);
      if (curPos > curRGEnd) {
        curRGIdx++;
        continue;
      }
      ScanMode cursm = resultSMs[curRGIdx];
      if (cursm == ScanMode.Positive) {
        return curPos++;
      } else if (cursm == ScanMode.Negative) {
        if (curRGIdx == rgs.size() - 1) {
          return -1;
        }
        else {
          curPos = rgs.get(curRGIdx) + 1;
          curRGIdx++;
          continue;
        }
      } else { // cursm == ScanMode.Rough
        List<ValPair> curRecord = getVpsAtPos(filterCfs, curPos);
        if (curRecord == null) {
          return -1;
        }
        if (cachedEvaluator == null) {
          cachedEvaluator = new RowEvaluator(tblDesc, filterDesc);
        }
        Boolean res = cachedEvaluator.evaluate(curRecord, filterCfs, filterColumns);
        if (Boolean.TRUE.equals(res)) {
          reporter.incrCounter(FilterState.ROUGH_PASSED, 1);
          return curPos++;
        } else {
          reporter.incrCounter(FilterState.ROUGH_FILTERED, 1);
          curPos++;
        }
      } // end else
    }// end while
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

  private enum FilterState {
    ALL, GEN_ALL, POS_PASSED, ROUGH_PASSED, NEG_FILTERED, ROUGH_FILTERED
  }

  private enum RG {
    POSITIVE, NEGATIVE, ROUGH
  }

}
