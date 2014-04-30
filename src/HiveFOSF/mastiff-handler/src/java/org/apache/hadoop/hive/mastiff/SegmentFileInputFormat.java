package org.apache.hadoop.hive.mastiff;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.MTableDesc;
import org.apache.hadoop.hive.mastiff.SegmentFile.SegmentIndexReader;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import cn.ac.ncic.mastiff.etl.ETLUtils;
import cn.ac.ncic.mastiff.io.segmentfile.PageMeta;
import cn.ac.ncic.mastiff.io.segmentfile.PageMeta.ScanMode;
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
  private static HashMap<String, Set<String>> rackToNodes =
      new HashMap<String, Set<String>>();

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
    String tblName = job.get(MastiffHandlerUtil.CF_TABLE_NAME);

    if (filterExprSerialized == null || filterExprSerialized.equals("")) {
      filter = null;
    }

    if (filterExprSerialized != null && !filterExprSerialized.equals("")) {
      filter = Utilities.deserializeExpression(filterExprSerialized, job);

      try {
        MastiffHandlerUtil.setCFMeta(conf, tblName);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      tblDesc = MastiffHandlerUtil.getMTableDesc(job);
      MastiffHandlerUtil.getCFTypes(tblDesc);
    }

    FileStatus[] files = listStatus(job);

    ArrayList<SegmentFileSplit> splits = new ArrayList<SegmentFileSplit>();
    boolean withPageMeta = MastiffMapReduce.getReadMastiffPageMeta(job);

    int n_seg = 0;
    int r_seg = 0;
    int p_seg = 0;

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

      if (filter != null) {
        // if (filter != null && MastiffHandlerUtil.isColInTable(tblName, filter)) {
        // get filter cols & cfs
        List<Integer> filterColumns = new ArrayList<Integer>();
        List<Integer> filterCfs = new ArrayList<Integer>();
        MastiffHandlerUtil.getFilterInfo(tblDesc, filter, filterCfs, filterColumns);

        PageMeta[][] segMetas = sir.getSegMetas();
        for (int sgIdx = 0; sgIdx < segmentOffsets.length; sgIdx++) {
          segmentScanModes[sgIdx] = MastiffHandlerUtil.getScanModeForSegment(tblDesc, filter,
              segMetas[sgIdx]);
        }
      } else {
        for (int i = 0; i < segmentOffsets.length; i++) {
          segmentScanModes[i] = ScanMode.Positive;
        }
      }


      for (int curIdx = 0; curIdx < segmentOffsets.length; curIdx++) {
        if (segmentScanModes != null && segmentScanModes[curIdx] == ScanMode.Negative) {
          n_seg++;
          continue;
        } else if (segmentScanModes != null && segmentScanModes[curIdx] == ScanMode.Rough) {
          r_seg++;
        } else if (segmentScanModes != null && segmentScanModes[curIdx] == ScanMode.Positive) {
          p_seg++;
        }
        int blkIndex = getBlockIndex(blkLocations, segmentOffsets[curIdx]);
        // LOG.info("Add a segment split : offset " + segmentOffsets[curIdx] + " length " +
        // segmentLengths[curIdx] + " : pms offset " + segmentPMSOffsets[curIdx]);
        splits.add(new SegmentFileSplit(segId + curIdx, path, segmentOffsets[curIdx],
            segmentPMSOffsets[curIdx], segmentLengths[curIdx],
            segmentLengths[curIdx], blkLocations[blkIndex], tblName));
      }
    }
    LOG.info("Total # of Positive Segment : " + p_seg);
    LOG.info("Total # of Rough Segment : " + r_seg);
    LOG.info("Total # of Negative Segment : " + n_seg);
    LOG.info("Total # of SegmentFileSplit : " + splits.size());
    // return splits.toArray(new SegmentFileSplit[0]);
    List<SegmentFileCombineSplit> combineResults = new ArrayList<SegmentFileCombineSplit>();
    long maxSize = conf.getLong("mastiff.mapred.max.split.size", 268435456L);
    long minSizeNode = conf.getLong("mastiff.mapred.min.split.size.per.node", 1L);
    long minSizeRack = conf.getLong("mastiff.mapred.min.split.size.per.rack", 1L);

    getMoreSplits(maxSize, minSizeNode, minSizeRack, splits, combineResults);
    LOG.info("Total # of SegmentFileCombineSplit : " + combineResults.size());
    return combineResults.toArray(new SegmentFileCombineSplit[0]);
  }

  /**
   * Combine local SegmentFileSplit to generate larger ones
   *
   * The generated splits would mostly have size greater than <code>maxsize</code>,
   * so, If no combination is in need, just change maxSize to 1
   * Adapted from {@link org.apache.hadoop.mapred.lib.CombineFileInputFormat}
   *
   * @throws IOException
   */
  protected void getMoreSplits(long maxSize, long minSizeNode, long minSizeRack,
      List<SegmentFileSplit> insplits, List<SegmentFileCombineSplit> outsplits) throws IOException {

    // mapping from rack name to list of splits it has
    HashMap<String, List<SegmentFileSplit>> rackToSplits =
        new HashMap<String, List<SegmentFileSplit>>();

    // mapping from segmentsplit to the nodes on which it has replicas
    HashMap<SegmentFileSplit, String[]> splitToNodes =
        new HashMap<SegmentFileSplit, String[]>();

    // mapping from node to the list of splits it contains
    HashMap<String, List<SegmentFileSplit>> nodeToSplits =
        new HashMap<String, List<SegmentFileSplit>>();

    initMapping(insplits, rackToSplits, splitToNodes, nodeToSplits);

    List<SegmentFileSplit> validSegment = new ArrayList<SegmentFileSplit>();
    List<String> nodes = new ArrayList<String>();
    long curSplitSize = 0;

    // process all segmentsplit local to a node
    for (Entry<String, List<SegmentFileSplit>> curNodeToSplits : nodeToSplits.entrySet()) {
      nodes.add(curNodeToSplits.getKey());
      List<SegmentFileSplit> splitInCurNode = curNodeToSplits.getValue();
      for (SegmentFileSplit curSfs : splitInCurNode) {
        if (splitToNodes.containsKey(curSfs)) {
          validSegment.add(curSfs);
          splitToNodes.remove(curSfs);
          curSplitSize += curSfs.segmentLength;

          if (maxSize != 0 && curSplitSize >= maxSize) {
            addCreatedSplit(validSegment, nodes, outsplits);
            curSplitSize = 0;
            validSegment.clear();
          }
        }
      }
      if (minSizeNode != 0 && curSplitSize > minSizeNode) {
        addCreatedSplit(validSegment, nodes, outsplits);
      } else {
        for (SegmentFileSplit sfs : validSegment) {
          splitToNodes.put(sfs, sfs.hosts);
        }
      }
      validSegment.clear();
      curSplitSize = 0;
      nodes.clear();
    } // end process nodes' local segmentFile

    // combine splits in each rack until total size smaller than minRackSize
    // leave them into 'overflow', After all the racks are processed, combine the
    // overflow segments into combinesplits
    List<SegmentFileSplit> overflowSegments = new ArrayList<SegmentFileSplit>();
    List<String> racks = new ArrayList<String>();

    //
    while (splitToNodes.size() > 0) {
      for (Entry<String, List<SegmentFileSplit>> curRackToSplit : rackToSplits.entrySet()) {
        racks.add(curRackToSplit.getKey());
        List<SegmentFileSplit> splitsInCurRack = curRackToSplit.getValue();

        boolean createdSplit = false;
        for (SegmentFileSplit curSfs : splitsInCurRack) {
          if (splitToNodes.containsKey(curSfs)) {
            validSegment.add(curSfs);
            splitToNodes.remove(curSfs);
            curSplitSize += curSfs.segmentLength;

            if (maxSize != 0 && curSplitSize >= maxSize) {
              addCreatedSplit(validSegment, getHosts(racks), outsplits);
              createdSplit = true;
              break;
            }
          }
        }
        if (createdSplit) {
          curSplitSize = 0;
          validSegment.clear();
          racks.clear();
          continue;
        }

        if (!validSegment.isEmpty()) {
          if (minSizeRack != 0 && curSplitSize >= minSizeRack) {
            addCreatedSplit(validSegment, getHosts(racks), outsplits);
          } else {
            overflowSegments.addAll(validSegment);
          }
        }
        curSplitSize = 0;
        validSegment.clear();
        racks.clear();
      }
    }

    assert splitToNodes.isEmpty();
    assert curSplitSize == 0;
    assert racks.isEmpty();
    assert validSegment.isEmpty();

    // process all overflow splits
    for (SegmentFileSplit curSfs : overflowSegments) {
      validSegment.add(curSfs);
      curSplitSize += curSfs.segmentLength;

      String[] curRacks = genRack(curSfs.hosts, curSfs.blkLocation);
      for (int i = 0; i < curRacks.length; i++) {
        racks.add(curRacks[i]);
      }

      if (maxSize != 0 && curSplitSize >= maxSize) {
        addCreatedSplit(validSegment, getHosts(racks), outsplits);
        curSplitSize = 0;
        validSegment.clear();
        racks.clear();
      }
    }

    // process all remaining splits
    if (!validSegment.isEmpty()) {
      addCreatedSplit(validSegment, getHosts(racks), outsplits);
    }

  }

  private void initMapping(List<SegmentFileSplit> insplits,
      HashMap<String, List<SegmentFileSplit>> rackToSplits,
      HashMap<SegmentFileSplit, String[]> splitToNodes,
      HashMap<String, List<SegmentFileSplit>> nodeToSplits) throws IOException {

    for (SegmentFileSplit sfs : insplits) {
      // split to host map
      splitToNodes.put(sfs, sfs.hosts);
      BlockLocation curLoc = sfs.blkLocation;

      String[] hosts = sfs.hosts;

      String[] racks = genRack(hosts, curLoc);

      // rack to split map
      for (int ri = 0; ri < racks.length; ri++) {
        String rack = racks[ri];
        List<SegmentFileSplit> splitList = rackToSplits.get(rack);
        if (splitList == null) {
          splitList = new ArrayList<SegmentFileSplit>();
          rackToSplits.put(rack, splitList);
        }
        splitList.add(sfs);

        addHostToRack(racks[ri], hosts[ri]);
      }

      // host to split map
      for (int hi = 0; hi < hosts.length; hi++) {
        String node = hosts[hi];
        List<SegmentFileSplit> splitList = nodeToSplits.get(node);
        if (splitList == null) {
          splitList = new ArrayList<SegmentFileSplit>();
          nodeToSplits.put(node, splitList);
        }
        splitList.add(sfs);
      }
    }
  }

  private void addCreatedSplit(List<SegmentFileSplit> parts, List<String> hosts,
      List<SegmentFileCombineSplit> outsplits) {
    SegmentFileCombineSplit sfcs = new SegmentFileCombineSplit(parts, hosts.toArray(new String[0]));
    outsplits.add(sfcs);
  }

  private String[] genRack(String[] hosts, BlockLocation curLoc) throws IOException {

    String[] topoPath = curLoc.getTopologyPaths();
    if (topoPath.length == 0) {
      topoPath = new String[hosts.length];
      for (int i = 0; i < hosts.length; i++) {
        topoPath[i] = (new NodeBase(hosts[i], NetworkTopology.DEFAULT_RACK)).toString();
      }
    }
    String[] racks = new String[topoPath.length];
    for (int i = 0; i < racks.length; i++) {
      racks[i] = (new NodeBase(topoPath[i])).getNetworkLocation();
    }
    return racks;
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

  private static void addHostToRack(String rack, String host) {
    Set<String> hosts = rackToNodes.get(rack);
    if (hosts == null) {
      hosts = new HashSet<String>();
      rackToNodes.put(rack, hosts);
    }
    hosts.add(host);
  }

  private static List<String> getHosts(List<String> racks) {
    List<String> hosts = new ArrayList<String>();
    for (String rack : racks) {
      hosts.addAll(rackToNodes.get(rack));
    }
    return hosts;
  }

}
