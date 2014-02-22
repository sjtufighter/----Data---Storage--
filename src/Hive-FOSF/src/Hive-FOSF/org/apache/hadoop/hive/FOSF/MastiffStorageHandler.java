package org.apache.hadoop.hive.mastiff;

import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

/**
 *
 * MastiffStorageHandler provides a HiveStorageHandler implementation for
 * Segment File
 *  @author shenyiji  wangmeng
 *
 */
public class MastiffStorageHandler extends DefaultStorageHandler implements
    HiveStoragePredicateHandler, HiveMetaHook{

  public static Log LOG = LogFactory.getLog(MastiffStorageHandler.class);

  private Configuration conf;
  private String mastiffMetastoreIp = null;
  private MastiffMetastoreClient mmclient = null;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return SegmentFileInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return SegmentFileOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return MastiffSerDe.class;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {

    Properties tableProperties = tableDesc.getProperties();
    String tblName = tableProperties.getProperty("name");
    jobProperties.put(MastiffHandlerUtil.CF_TABLE_NAME, tblName);

  }

  /**
   * In order to user filter in building ScanMap and record filter at record level,
   *  we need filter passed into InputFormat as well as recordReader
   */
  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = predicate.clone();
    decomposedPredicate.residualPredicate = null;
    return decomposedPredicate;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException{
    init_mms();
    Hashtable meta = mmclient.getMetadata(table.getTableName());
    if(meta != null) {
      throw new MetaException("Table with name:" + table.getTableName() + " already exist");
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    init_mms();
    String tableName = table.getTableName();
    Map<String, String> serdeInfoParams = table.getSd().getSerdeInfo().getParameters();
    String columnmap = serdeInfoParams.get(MastiffHandlerUtil.CF_COLUMN_MAPPING);
    String codingtype = serdeInfoParams.get(MastiffHandlerUtil.CF_CODING);
    String algorithm = serdeInfoParams.get(MastiffHandlerUtil.CF_ALGO);
    if(columnmap == null || codingtype == null || algorithm == null) {
      throw new MetaException("Mastiff Metadata incomplete");
    }
    boolean succ = mmclient.create_mmtable(tableName, columnmap, algorithm, codingtype);
    if(!succ) {
      throw new MetaException("Fail to insert metadata into mastiff metastore");
    }
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    init_mms();
    String tableName = table.getTableName();
    mmclient.delete_mmtable(tableName);
  }

  public void init_mms() throws MetaException {
    if(mastiffMetastoreIp == null) {
      mastiffMetastoreIp = conf.get("mastiff.metastore.ip");
    }
    if(mastiffMetastoreIp == null) {
      throw new MetaException("Mastiff Metastore ip not set in hive-site.xml");
    }
    if(mmclient == null) {
      mmclient = new MastiffMetastoreClient(mastiffMetastoreIp);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
