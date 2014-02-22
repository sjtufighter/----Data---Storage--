package org.apache.hadoop.hive.mastiff;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import cn.ac.ncic.mastiff.MConstants;
import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.io.coding.Compression.Algorithm;
import cn.ac.ncic.mastiff.io.coding.Encoder.CodingType;
import cn.ac.ncic.mastiff.io.segmentfile.PageMeta;
import cn.ac.ncic.mastiff.io.segmentfile.PageMeta.ScanMode;
import cn.ac.ncic.mastiff.io.segmentfile.PageMetaList;
import cn.ac.ncic.mastiff.io.segmentfile.PageMetaSection;

/*Practical configuration
 *
 */
public class MastiffHandlerUtil {

  public static final String CF_COLUMN_MAPPING = "mastiff.table.columns.map";
  public static final String CF_ALGO = "mastiff.table.algorithms";
  public static final String CF_CODING = "mastiff.table.codingtypes";
  public static final String CF_TABLE_NAME = "mastiff.table.name";

  public static final Log LOG =
      LogFactory.getLog(MastiffHandlerUtil.class);

  private static ObjectInspector cachedMastiffRowOI = null;


  public static class MTableDesc{
    public String[] columnNames = null;
    public TypeInfo[] columnTypes = null;
    public Algorithm[] clusterAlgos = null;
    public CodingType[] clusterCodingTypes = null;
    public int[][] columnsMapping = null;
    public TypeInfo[][] clusterTypes = null;
  }

  public static class ColumnDesc {
    // column family idx
    public int cf;
    // column idx in the current cluster
    public int idxInCf;
    // column idx in table
    public int idxInTbl;
  }

  public static class ValidColumnsInCF {
    public int cf;
    public List<Integer> validColumns = new ArrayList<Integer>();
  }

  /**
   * Set cf metadata into configuration from Mastiff MetaStore
   * @throws Exception
   */
  public static void setCFMeta(Configuration conf, String tableName) throws Exception {
    String mmsip = conf.get("mastiff.metastore.ip");
    if(mmsip == null) {
      throw new Exception("Mastiff Metastore ip not set in hive-site.xml");
    }
    MastiffMetastoreClient mmc = new MastiffMetastoreClient(mmsip);
    Hashtable meta = null;
    meta = mmc.getMetadata(tableName);
    if(meta != null) {
      conf.set(CF_COLUMN_MAPPING, (String) meta.get("columnsmap"));
      conf.set(CF_ALGO, (String) meta.get("algorithm"));
      conf.set(CF_CODING, (String) meta.get("codingtype"));
    } else {
      throw new Exception("No such table exist");
    }
  }


  /**
   * Get Column Family definitions from configuration
   * including each column's compression Algo, coding type and column Mapping
   */
  public static MTableDesc getMTableDesc(Object obj) {
    MTableDesc desc = new MTableDesc();

    StringTokenizer st = null;

    String str_columnmap, str_algorithms ,str_codingtypes;

    getColumnInfos(desc, obj);

    if(obj instanceof Configuration) {
      Configuration conf = (Configuration) obj;
      str_columnmap = conf.get(MastiffHandlerUtil.CF_COLUMN_MAPPING);
      str_algorithms = conf.get(MastiffHandlerUtil.CF_ALGO);
      str_codingtypes = conf.get(MastiffHandlerUtil.CF_CODING);
    } else if (obj instanceof Properties) {
      Properties props = (Properties) obj;
      str_columnmap = props.getProperty(MastiffHandlerUtil.CF_COLUMN_MAPPING);
      str_algorithms = props.getProperty(MastiffHandlerUtil.CF_ALGO);
      str_codingtypes = props.getProperty(MastiffHandlerUtil.CF_CODING);
    } else {
      return null;
    }

    if (str_columnmap != null) {
      st = new StringTokenizer(str_columnmap, MConstants.CLUSTER_SEP);
      desc.columnsMapping = new int[st.countTokens()][];
      int i = 0;
      while (st.hasMoreTokens()) {
        StringTokenizer st1 = new StringTokenizer(st.nextToken(), MConstants.COLUMN_SEP);
        desc.columnsMapping[i] = new int[st1.countTokens()];
        int j = 0;
        while (st1.hasMoreTokens()) {
          desc.columnsMapping[i][j] = Integer.valueOf(st1.nextToken());
          j++;
        }
        i++;
      }
    }

    desc.clusterAlgos = new Algorithm[desc.columnsMapping.length];
    desc.clusterCodingTypes = new CodingType[desc.columnsMapping.length];

    if (str_algorithms != null) {
      st = new StringTokenizer(str_algorithms, MConstants.CLUSTER_SEP);
      int i = 0;
      while (st.hasMoreTokens()) {
        desc.clusterAlgos[i] = Algorithm.valueOf(st.nextToken());
        i++;
      }
    }

    st = new StringTokenizer(str_codingtypes, MConstants.CLUSTER_SEP);
    int i = 0;
    while (st.hasMoreTokens()) {
      desc.clusterCodingTypes[i] = CodingType.valueOf(st.nextToken());
      i++;
    }

    return desc;
  }


  /**
   * Get each CF's fields' data types
   * @param desc
   */
  public static void getCFTypes(MTableDesc desc) {
    TypeInfo[] clmTypes = desc.columnTypes;
    desc.clusterTypes = new TypeInfo[desc.columnsMapping.length][];
    for(int i = 0; i < desc.columnsMapping.length; i++) {
      desc.clusterTypes[i] = new TypeInfo[desc.columnsMapping[i].length];
      for(int j =0; j < desc.columnsMapping[i].length; j++) {
        desc.clusterTypes[i][j] = clmTypes[desc.columnsMapping[i][j]];
      }
    }
  }


  public static void getColumnInfos(MTableDesc tbl, Object obj) {
    String str_columnNames = null;
    String str_columnTypes = null;

    if(obj instanceof Configuration) {
      Configuration conf = (Configuration) obj;
      str_columnNames = conf.get(serdeConstants.LIST_COLUMNS);
      str_columnTypes = conf.get(serdeConstants.LIST_COLUMN_TYPES);
    } else if (obj instanceof Properties) {
      Properties props = (Properties) obj;
      str_columnNames = props.getProperty(serdeConstants.LIST_COLUMNS);
      str_columnTypes = props.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    }

    List<String> columnNames = null;
    List<TypeInfo> columnTypes = null;

    if (str_columnNames != null && str_columnNames.length() > 0) {
      columnNames = Arrays.asList(str_columnNames.split(","));
    } else {
      columnNames = new ArrayList<String>();
    }
    if (str_columnTypes == null) {
      // Default type: all string
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < columnNames.size(); i++) {
        if (i > 0) {
          sb.append(":");
        }
        sb.append(serdeConstants.STRING_TYPE_NAME);
      }
      str_columnTypes = sb.toString();
    }

    columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(str_columnTypes);

    int vcnum = 0;
    if(columnNames.contains(VirtualColumn.FILENAME.getName())) {
      vcnum++;
    }
    if(columnNames.contains(VirtualColumn.GROUPINGID.getName())) {
      vcnum++;
    }
    if(columnNames.contains(VirtualColumn.RAWDATASIZE.getName())) {
      vcnum++;
    }
    if(columnNames.contains(VirtualColumn.ROWOFFSET.getName())) {
      vcnum++;
    }
    if(columnNames.contains(VirtualColumn.BLOCKOFFSET.getName())) {
      vcnum++;
    }

    columnNames = columnNames.subList(0, columnNames.size()-vcnum);
    columnTypes = columnTypes.subList(0, columnTypes.size()-vcnum);

    tbl.columnNames = columnNames.toArray(new String[0]);
    tbl.columnTypes = columnTypes.toArray(new TypeInfo[0]);
  }

  /*
   * Found which column family the column belongs to
   * and the column's position in that column family
   */
  public static ColumnDesc getCF(MTableDesc tbl, int colIdx) {
    int[][] cm = tbl.columnsMapping;
    ColumnDesc cd = new ColumnDesc();
    cd.idxInTbl = colIdx;
    for (int cfIdx = 0; cfIdx < cm.length; cfIdx++) {
      for (int colInCfIdx = 0; colInCfIdx < cm[cfIdx].length; colInCfIdx++) {
        if (colIdx == cm[cfIdx][colInCfIdx]) {
          cd.cf = cfIdx;
          cd.idxInCf = colInCfIdx;
          return cd;
        }
      }
    }
    return null;
  }

  /**
   * Find each column's column family
   */
  public static List<Integer> getCFs(MTableDesc tbl, List<Integer> cols) {
    List<Integer> CFs = new ArrayList<Integer>();
    for (Integer col : cols) {
      ColumnDesc cd = getCF(tbl, col);
      if (cd != null) {
        CFs.add(cd.cf);
      }
      else {
        CFs.add(-1);
      }
    }
    return CFs;
  }

  public static HashMap<Integer, ValidColumnsInCF> getCfValidColumns(MTableDesc tbl,
      List<Integer> validCols) {
    HashMap<Integer, ValidColumnsInCF> result = new HashMap<Integer, ValidColumnsInCF>();
    for (Integer col : validCols) {
      ColumnDesc cd = getCF(tbl, col);
      if (cd != null) {
        ValidColumnsInCF vcic = result.get(cd.cf);
        if (vcic == null) {
          vcic = new ValidColumnsInCF();
          vcic.cf = cd.cf;
          vcic.validColumns.add(cd.idxInCf);
          result.put(cd.cf, vcic);
        }
        else {
          vcic.validColumns.add(cd.idxInCf);
        }
      }
    }
    return result;
  }

  public static void loadPropertiesFromFile(Properties props, String filePath) throws FileNotFoundException, IOException {
    InputStream in = new BufferedInputStream(new FileInputStream(filePath));
    props.load(in);
  }



  public static String getTableNameFromFilter(ExprNodeDesc filter) {
    String tblAlias = null;
    if (filter instanceof ExprNodeColumnDesc) {
      return ((ExprNodeColumnDesc)filter).getTabAlias();
    }
    else if (filter instanceof ExprNodeGenericFuncDesc) {
      List<ExprNodeDesc> children = ((ExprNodeGenericFuncDesc) filter).getChildExprs();
      for (ExprNodeDesc child : children) {
        tblAlias = getTableNameFromFilter(child);
        if (tblAlias != null) {
          return tblAlias;
        }
      }
    }
    return null;
  }

  public static Set<String> getColumnNamesFromFilter(ExprNodeDesc filter) {
    Set<String> colNames = new HashSet<String>();
    if(filter instanceof ExprNodeColumnDesc) {
      colNames.add(((ExprNodeColumnDesc) filter).getColumn());
    }
    else if(filter instanceof ExprNodeGenericFuncDesc) {
      List<ExprNodeDesc> children = ((ExprNodeGenericFuncDesc) filter).getChildExprs();
      for (ExprNodeDesc child : children) {
        colNames.addAll(getColumnNamesFromFilter(child));
      }
    }
    return colNames;
  }

  public static List<Integer> getIdxFromName(MTableDesc tblDesc, List<String> filterColumnsLst) {
    List<Integer> cols = new ArrayList<Integer>();
    HashMap<String, Integer> nameToIdx = new HashMap<String, Integer>();
    for(int i = 0; i < tblDesc.columnNames.length; i++) {
      nameToIdx.put(tblDesc.columnNames[i], i);
    }
    for(String colName : filterColumnsLst) {
      cols.add(nameToIdx.get(colName));
    }
    return cols;
  }

  public static List<Integer> getColIdFromFilter(MTableDesc tblDesc, ExprNodeDesc filter) {
    List<String> filterColumnsLst = new ArrayList<String>(MastiffHandlerUtil.getColumnNamesFromFilter(filter));
    List<Integer> filterColumns = MastiffHandlerUtil.getIdxFromName(tblDesc, filterColumnsLst);
    Collections.sort(filterColumns);
    return filterColumns;
  }

  public static void getFilterInfo(MTableDesc tblDesc, ExprNodeDesc filter, List<Integer> filterCfs, List<Integer> filterColumns) {
    filterColumns.addAll(MastiffHandlerUtil.getColIdFromFilter(tblDesc, filter));
    HashMap<Integer, ValidColumnsInCF> cfWithCols = MastiffHandlerUtil.getCfValidColumns(tblDesc, filterColumns);
    for (Integer key : cfWithCols.keySet()) {
      filterCfs.add(key);
    }
  }

  public static boolean isColInTable(String tblName, ExprNodeDesc filter) {
    String tblAlias = null;
    String[] parts = tblName.split("\\.");
    if(tblName.contains(".")) {
      tblAlias = parts[1];
    } else {
      tblAlias = parts[0];
    }
    if(tblAlias.equalsIgnoreCase(getTableNameFromFilter(filter))) {
      return true;
    }
    else {
      return false;
    }
  }

  public static Boolean evaluate(MTableDesc tblDesc, ExprNodeDesc filter,
      List<Integer> filterCfs, List<Integer> filterColumns, List<ValPair> vps) {

    if(cachedMastiffRowOI == null) {
      cachedMastiffRowOI = MastiffSerDe.createMastiffRowObjectInspector(
          Arrays.asList(tblDesc.columnNames), Arrays.asList(tblDesc.columnTypes));
    }

    RowWritable rw = new RowWritable();
    rw.set((byte) 1, vps, filterCfs, filterColumns);
    LazyMastiffRow lmr = new LazyMastiffRow(
        (LazyMastiffRowObjectInspector) cachedMastiffRowOI);
    lmr.init(rw, tblDesc);

    ExprNodeEvaluator conditionEvaluator = ExprNodeEvaluatorFactory.get(filter);
    PrimitiveObjectInspector conditionOI;
    Boolean ret = null;
    try {
      conditionOI = (PrimitiveObjectInspector) conditionEvaluator
          .initialize(cachedMastiffRowOI);
    Object cond = conditionEvaluator.evaluate(lmr);
    ret = (Boolean) conditionOI.getPrimitiveJavaObject(cond);

    } catch (HiveException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return ret;
  }

  public static List<Integer> getFittestSplit(List<Integer> filterCfs, PageMetaSection pms) {
    PageMetaList[] filterPML = new PageMetaList[filterCfs.size()];
    PageMetaList[] allPML = pms.getPageMetaLists();

    List<Integer> curPageIdxInEachCf = new ArrayList<Integer>();
    List<Integer> pageNumInEachCf = new ArrayList<Integer>();

    int curRGEnd = 0;
    List<Integer> rgLst = new ArrayList<Integer>();

    int i = 0;
    for(Integer cf : filterCfs) {
      filterPML[i++] = allPML[cf];
      curPageIdxInEachCf.add(0);
      pageNumInEachCf.add(allPML[cf].getMetaList().size());
    }
//    LOG.error("Page num in each cf "+pageNumInEachCf);
    while(checkPageIdx(pageNumInEachCf, curPageIdxInEachCf)) {
      rgLst.add(findSmallestEnd(filterPML, curPageIdxInEachCf));
    }//end while
//    return getEndsAsOrdersList(filterPML);
    return rgLst;
  }

//  public static List<Integer> getEndsAsOrdersList(PageMetaList[] filterPMLs) {
//    Set<Integer> endset = new TreeSet<Integer>();
//    for(int i = 0; i < filterPMLs.length; i++) {
//      List<PageMeta> curPML = filterPMLs[i].getMetaList();
//      for(PageMeta curPM : curPML) {
//        endset.add(curPM.startPos + curPM.numPairs - 1);
//      }
//    }
//    return new ArrayList<Integer>(endset);
//  }

  public static int findSmallestEnd(PageMetaList[] filterPML, List<Integer> curPageIdxInEachCf) {
    List<Integer> curPageEnd = new ArrayList<Integer>();
    for(int i = 0; i < filterPML.length; i++) {
      PageMeta curPM = filterPML[i].getMetaList().get(curPageIdxInEachCf.get(i));
      curPageEnd.add(curPM.startPos + curPM.numPairs - 1);
    }
    int smallestEnd = Collections.min(curPageEnd);
    for(int i = 0; i < filterPML.length; i++) {
      if(curPageEnd.get(i) == smallestEnd) {
        curPageIdxInEachCf.set(i, curPageIdxInEachCf.get(i)+1);
      }
    }
//    LOG.error("Cur page Idx in each cf "+curPageIdxInEachCf);
    return smallestEnd;
  }

  public static boolean checkPageIdx(List<Integer> pageNumInEachCf, List<Integer> curPageIdxInEachCf) {
    for(int i = 0; i < pageNumInEachCf.size(); i++) {
//      LOG.error("Cur Idx "+curPageIdxInEachCf+"_"+pageNumInEachCf);
//      LOG.error("If equals "+(curPageIdxInEachCf.get(i) == pageNumInEachCf.get(i)));
      if(curPageIdxInEachCf.get(i).equals(pageNumInEachCf.get(i))) {
//        LOG.error("will return false");
        return false;
      }
    }
    return true;
  }

  public static ScanMode[] getScanModesForRG(MTableDesc tblDesc, ExprNodeDesc filter, PageMetaSection pms, List<Integer> rgs) {
    Stack<ScanMode[]> tmpResult = new Stack<ScanMode[]>();
    getScanModesForRG(tblDesc, tmpResult, filter, pms, rgs);
    return tmpResult.pop();
  }

  public static void getScanModesForRG(MTableDesc tblDesc,
      Stack<ScanMode[]> tmpResult, ExprNodeDesc filter, PageMetaSection pms, List<Integer> rgs) {
    boolean leaf = false;
    if(filter instanceof ExprNodeGenericFuncDesc) {
      GenericUDF udf = ((ExprNodeGenericFuncDesc) filter).getGenericUDF();
      String udfname = udf.getClass().getSimpleName();
      String regex = "UDFOP(.*)$";
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(udfname);
      m.find();
      UDFType type = UDFType.fromString(m.group(1).toLowerCase());

      List<ExprNodeDesc> children = ((ExprNodeGenericFuncDesc) filter).getChildExprs();
      for(ExprNodeDesc child : children) {
        if(child instanceof ExprNodeColumnDesc || child instanceof ExprNodeConstantDesc) {
          leaf = true;
        }
      }
      if(leaf) {
        // generate scanmap for rg
        List<Integer> col = getColIdFromFilter(tblDesc, filter);
        assert(col.size() == 1);
        ColumnDesc cd = getCF(tblDesc, col.get(0));

        PageMetaList pml = pms.getPageMetaLists()[cd.cf];
        List<PageMeta> mts = pml.getMetaList();
        ScanMode[] sms = new ScanMode[mts.size()];
        int i = 0;
        for(PageMeta pm: mts) {
          Boolean maxret = evaluate(tblDesc, filter, Arrays.asList(cd.cf), Arrays.asList(cd.idxInTbl), Arrays.asList(pm.max));
          Boolean minret = evaluate(tblDesc, filter, Arrays.asList(cd.cf), Arrays.asList(cd.idxInTbl), Arrays.asList(pm.min));
          if(Boolean.TRUE.equals(maxret) && Boolean.TRUE.equals(minret)) {
            sms[i++] = ScanMode.Positive;
          } else if(Boolean.FALSE.equals(maxret) && Boolean.FALSE.equals(minret)) {
            switch(type) {
            case EQ:
              sms[i++] = intoEqual(tblDesc, filter, cd, pm);
              break;
            default:
              sms[i++] = ScanMode.Negative;
            }
          } else {
            sms[i++] = ScanMode.Rough;
          }
        }
        ScanMode[] rgsms = extendScanModesToRG(sms, mts, rgs);
        tmpResult.push(rgsms);
      }
      else {
        for(ExprNodeDesc child : children) {
          getScanModesForRG(tblDesc, tmpResult, child, pms, rgs);
        }
       ScanMode[] result = null;
       switch(type) {
       case AND:
         result = and(tmpResult.pop(), tmpResult.pop());
         break;
       case OR:
         result = or(tmpResult.pop(), tmpResult.pop());
         break;
       case NOT:
         result = not(tmpResult.pop());
         break;
       default:
         throw new UnsupportedOperationException("Do not support this expression: "
             + filter.getExprString());
       }
       tmpResult.push(result);
      }
    }
  }//end method

  public static ScanMode getScanModeForSegment(MTableDesc tblDesc, ExprNodeDesc filter, PageMeta[] cfMts) {
    Stack<ScanMode[]> tmpResult = new Stack<ScanMode[]>();
    getScanModeForSegment(tblDesc, tmpResult, filter, cfMts);
    return tmpResult.pop()[0];
  }

  public static void getScanModeForSegment(MTableDesc tblDesc,
      Stack<ScanMode[]> tmpResult, ExprNodeDesc filter, PageMeta[] cfMts) {
    boolean leaf = false;
    if(filter instanceof ExprNodeGenericFuncDesc) {
      GenericUDF udf = ((ExprNodeGenericFuncDesc) filter).getGenericUDF();
      String udfname = udf.getClass().getSimpleName();
      String regex = "UDFOP(.*)$";
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(udfname);
      m.find();
      UDFType type = UDFType.fromString(m.group(1).toLowerCase());

      List<ExprNodeDesc> children = ((ExprNodeGenericFuncDesc) filter).getChildExprs();
      for(ExprNodeDesc child : children) {
        if(child instanceof ExprNodeColumnDesc || child instanceof ExprNodeConstantDesc) {
          leaf = true;
        }
      }
      if(leaf) {
        // generate scanmap for rg
        List<Integer> col = getColIdFromFilter(tblDesc, filter);
        assert(col.size() == 1);
        ColumnDesc cd = getCF(tblDesc, col.get(0));

        ScanMode sm = null;
        int i = 0;
        PageMeta pm = cfMts[cd.cf];
        Boolean maxret = evaluate(tblDesc, filter, Arrays.asList(cd.cf), Arrays.asList(cd.idxInTbl), Arrays.asList(pm.max));
        Boolean minret = evaluate(tblDesc, filter, Arrays.asList(cd.cf), Arrays.asList(cd.idxInTbl), Arrays.asList(pm.min));
        if(Boolean.TRUE.equals(maxret) && Boolean.TRUE.equals(minret)) {
          sm = ScanMode.Positive;
        } else if(Boolean.FALSE.equals(maxret) && Boolean.FALSE.equals(minret)) {
          switch(type) {
          case EQ:
            sm = intoEqual(tblDesc, filter, cd, pm);
            break;
          default:
            sm = ScanMode.Negative;
          }
        } else {
          sm = ScanMode.Rough;
        }
        tmpResult.push(new ScanMode[]{sm});
      }
      else {
        for(ExprNodeDesc child : children) {
          getScanModeForSegment(tblDesc, tmpResult, child, cfMts);
        }
       ScanMode[] result = null;
       switch(type) {
       case AND:
         result = and(tmpResult.pop(), tmpResult.pop());
         break;
       case OR:
         result = or(tmpResult.pop(), tmpResult.pop());
         break;
       case NOT:
         result = not(tmpResult.pop());
         break;
       default:
         throw new UnsupportedOperationException("Do not support this expression: "
             + filter.getExprString());
       }
       tmpResult.push(result);

      }
    }
  }

  private static ScanMode intoEqual(MTableDesc tblDesc, ExprNodeDesc filter, ColumnDesc cd,
      PageMeta pm) {
    ExprNodeGenericFuncDesc filterClone = (ExprNodeGenericFuncDesc) filter.clone();
    filterClone.setGenericUDF(new GenericUDFOPGreaterThan());
    ExprNodeGenericFuncDesc filterClone2 = (ExprNodeGenericFuncDesc) filter.clone();
    filterClone2.setGenericUDF(new GenericUDFOPLessThan());
    Boolean gt = evaluate(tblDesc, filterClone, Arrays.asList(cd.cf), Arrays.asList(cd.idxInTbl), Arrays.asList(pm.min));
    Boolean lt = evaluate(tblDesc, filterClone2, Arrays.asList(cd.cf), Arrays.asList(cd.idxInTbl), Arrays.asList(pm.max));
    if(Boolean.TRUE.equals(gt) || Boolean.TRUE.equals(lt)) {
      return ScanMode.Negative;
    } else {
      return ScanMode.Rough;
    }
  }


  public static ScanMode[] extendScanModesToRG(ScanMode[] sms, List<PageMeta> mts, List<Integer> rgs) {
    ScanMode[] rgsms = new ScanMode[rgs.size()];
    int idxInPage = 0;
    List<Integer> pageEnds = getPageEnds(mts);
    for(int idx = 0 ; idx < rgs.size(); idx++) {
      if(rgs.get(idx) <= pageEnds.get(idxInPage)) {
        rgsms[idx] = sms[idxInPage];
      } else if(rgs.get(idx) > pageEnds.get(idxInPage)) {
        idxInPage++;
        rgsms[idx] = sms[idxInPage];
      }
    }
    return rgsms;
  }

  public static List<Integer> getPageEnds(List<PageMeta> mts) {
    List<Integer> pageEnds = new ArrayList<Integer>();
    for(PageMeta pm : mts) {
      pageEnds.add(pm.startPos + pm.numPairs - 1);
    }
    return pageEnds;
  }

  public static ScanMode[] and(ScanMode[] left, ScanMode[] right) {
    ScanMode[] result = new ScanMode[left.length];
    for (int i = 0; i < left.length; i++) {
      if(left[i] == ScanMode.Negative || right[i] == ScanMode.Negative) {
        result[i] = ScanMode.Negative;
      } else if(left[i] == ScanMode.Rough || right[i] == ScanMode.Rough) {
        result[i] = ScanMode.Rough;
      } else {
        result[i] = ScanMode.Positive;
      }
    }
    return result;
  }

  public static ScanMode[] or(ScanMode[] left, ScanMode[] right) {
    ScanMode[] result = new ScanMode[left.length];
    for (int i = 0; i < left.length; i++) {
      if(left[i] == ScanMode.Positive || right[i] == ScanMode.Positive) {
        result[i] = ScanMode.Positive;
      } else if(left[i] == ScanMode.Rough || right[i] == ScanMode.Rough) {
        result[i] = ScanMode.Rough;
      } else {
        result[i] = ScanMode.Negative;
      }
    }
    return result;
  }

  public static ScanMode[] not(ScanMode[] left) {
    ScanMode[] result = new ScanMode[left.length];
    for (int i = 0; i < left.length; i++) {
      if(left[i] == ScanMode.Positive) {
        result[i] = ScanMode.Negative;
      } else if(left[i] == ScanMode.Negative) {
        result[i] = ScanMode.Positive;
      } else {
        result[i] = ScanMode.Rough;
      }
    }
    return result;
  }

  public static long getSegmentLineNum(PageMetaSection pms) {
    long count = 0;
    PageMetaList fstCF = pms.getPageMetaLists()[0];
    List<PageMeta> pml = fstCF.getMetaList();
    for(PageMeta pm : pml) {
      count += pm.numPairs;
    }
    return count;
  }

}
enum UDFType {
  LT("LessThan"),
  LT_EQ("EqualOrLessThan"),
  EQ("Equal"),
  GT_EQ("EqualOrGreaterThan"),
  GT("GreaterThan"),
  NOT("Not"),
  AND("And"),
  OR("Or");

  private final String text;

  UDFType(String text) {
    this.text = text;
  }

  public String getText() {
    return this.text;
  }

  public static UDFType fromString(String text) {
    if (text != null) {
      for (UDFType t : UDFType.values()) {
        if (text.equalsIgnoreCase(t.text)) {
          return t;
        }
      }
    }
    return null;
  }
}
