package org.apache.hadoop.hive.mastiff;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.MTableDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import cn.ac.ncic.mastiff.etl.ETLUtils;
import cn.ac.ncic.mastiff.io.segmentfile.PageMeta;
import cn.ac.ncic.mastiff.io.segmentfile.PageMeta.ScanMode;
import cn.ac.ncic.mastiff.io.segmentfile.SegmentFile.SegmentIndexReader;
import cn.ac.ncic.mastiff.mapred.MastiffMapReduce;

/**
 *
 * SegmentFileInputFormat is Adapted from
 * {@link cn.ac.ncic.mastiff.io.segmentfile.SegmentInputFormat} <br/>
 * Use filter passed from hive FilterPushDown in selecting possible Segment
 *
 * @param <K>
 * @param <V>
 */
public class SegmentFileInputFormat<K, V> extends FileInputFormat<K, V> {

  public static final String FILTER_EXPR_CONF_STR =
      "hive.io.filter.expr.serialized";
  public static final Log LOG =
      LogFactory.getLog(SegmentFileInputFormat.class);

  JobConf conf;
  ExprNodeDesc filter;
  MTableDesc tblDesc;

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    return new SegmentFileRecordReader(split, job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    conf = job;

    String filterExprSerialized = job.get(FILTER_EXPR_CONF_STR);

    if (filterExprSerialized != null) {
      filter = Utilities.deserializeExpression(filterExprSerialized, job);
    }

    String tblName = job.get(MastiffHandlerUtil.CF_TABLE_NAME);
    try {
      MastiffHandlerUtil.setCFMeta(conf, tblName);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    tblDesc = MastiffHandlerUtil.getMTableDesc(job);
    MastiffHandlerUtil.getCFTypes(tblDesc);

    FileStatus[] files = listStatus(job);

    ArrayList<SegmentFileSplit> splits = new ArrayList<SegmentFileSplit>();
    boolean withPageMeta = MastiffMapReduce.getReadMastiffPageMeta(job);

    for (FileStatus file : files) {
      Path path = file.getPath();
      FileSystem fs = path.getFileSystem(job);
      long fileLen = file.getLen();

      if (fileLen == 0) {
        continue;
      }

      // get the file id from its file name
      int fileId = -1;
      try {
        fileId = ETLUtils.getPartitionFromFileName(path.getName());
      } catch (ParseException pe) {
        throw new IOException(StringUtils.stringifyException(pe));
      }
      // form a segment id : segId = fileId << 16 + idx_in_current_file
      int segId = fileId << 16;

      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, fileLen);

      SegmentIndexReader sir = new SegmentIndexReader(fs, path, withPageMeta);
      sir.readSegIndex();
      sir.close();
      long[] segmentOffsets = sir.getSegOffsets();
      long[] segmentPMSOffsets = sir.getSegPMSOffsets();
      long[] segmentLengths = sir.getSegLengths();

      ScanMode[] segmentScanModes = null;
      segmentScanModes = new ScanMode[segmentOffsets.length];

      if(filter != null && MastiffHandlerUtil.isColInTable(tblName, filter)) {
        //get filter cols & cfs
        List<Integer> filterColumns = new ArrayList<Integer>();
        List<Integer> filterCfs = new ArrayList<Integer>();
        MastiffHandlerUtil.getFilterInfo(tblDesc, filter, filterCfs, filterColumns);

        PageMeta[][] segMetas = sir.getSegMetas();
        for (int sgIdx = 0; sgIdx < segmentOffsets.length; sgIdx++) {
          segmentScanModes[sgIdx] = MastiffHandlerUtil.getScanModeForSegment(tblDesc, filter, segMetas[sgIdx]);
        }
      } else {
        for(int i = 0; i < segmentOffsets.length; i++) {
          segmentScanModes[i] = ScanMode.Positive;
        }
      }

      for (int curIdx = 0; curIdx < segmentOffsets.length; curIdx++) {
        // if (segmentScanModes != null)
        // System.out.println("Segment " + curIdx + " is " + segmentScanModes[curIdx]);
        if (segmentScanModes != null && segmentScanModes[curIdx] == ScanMode.Negative) {
          continue;
        }
        int blkIndex = getBlockIndex(blkLocations, segmentOffsets[curIdx]);
        // LOG.info("Add a segment split : offset " + segmentOffsets[curIdx] + " length " +
        // segmentLengths[curIdx] + " : pms offset " + segmentPMSOffsets[curIdx]);
        splits.add(new SegmentFileSplit(segId + curIdx, path, segmentOffsets[curIdx],
            segmentPMSOffsets[curIdx], segmentLengths[curIdx],
            segmentLengths[curIdx], blkLocations[blkIndex].getHosts(), tblName));
      }
    }
    LOG.info("Total # of splits : " + splits.size());
    return splits.toArray(new SegmentFileSplit[splits.size()]);
  }

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    List<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    for (Path p : dirs) {
      FileSystem fs = p.getFileSystem(job);
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat : matches) {
          if (globStat.isDir()) {
            for (FileStatus stat : fs.listStatus(globStat.getPath(),
                inputFilter)) {
              result.add(stat);
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    LOG.info("Total input paths to process : " + result.size());
    return result.toArray(new FileStatus[result.size()]);
  }

  /**
   * Get the list of input {@link Path}s for the map-reduce job.
   *
   * @param conf
   *          The configuration of the job
   * @return the list of input {@link Path}s for the map-reduce job.
   */
  public static Path[] getInputPaths(JobConf conf) {
    String dirs = conf.get("mapred.input.dir");
    String[] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

  /**
   * Get a PathFilter instance of the filter set for the input paths.
   *
   * @return the PathFilter instance set for the job, NULL if none has been set.
   */
  public static PathFilter getInputPathFilter(JobConf conf) {
    Class<? extends PathFilter> filterClass = conf.getClass(
        "mapred.input.pathFilter.class", null, PathFilter.class);
    return (filterClass != null) ?
        ReflectionUtils.newInstance(filterClass, conf) : null;
  }


  private static final PathFilter hiddenFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Proxy PathFilter that accepts a path only if all filters given in the
   * constructor do. Used by the listPaths() to apply the built-in
   * hiddenFileFilter together with a user provided one (if any).
   */
  private static class MultiPathFilter implements PathFilter {
    private final List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    @Override
    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

}
