package org.apache.hadoop.hive.mastiff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.MTableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

import cn.ac.ncic.mastiff.etl.ETLUtils;


/**
 * MastiffSerDe is used for column family based storage supported by SegmentFile.
 */
public class MastiffSerDe extends AbstractSerDe {

  public static final String MASTIFF_CF_DEFINITION_PATH = "cf.config.path";
  public static final Log LOG = LogFactory.getLog(MastiffSerDe.class.getName());

  private Configuration job;
  private Properties tbl;

  private ObjectInspector cachedObjectInspector;
  //// private ArrayList<Object>  resuse =new ArrayList<Object>() ;
  private final ArrayList<OIG> resuse = new ArrayList<OIG>();
  private SerDeParameters serdeParams = null;
  private MTableDesc mtbl;
  private LazyMastiffRow cachedMastiffRow;
  private  boolean  isRowMapInit =false ;
  //private static   PrimitiveObjectInspector foi ;
  private  RowMap rowMap =null ;
  // public static long count =0;
  private   static int count=0;
  // public static long  intcount=0;
  // public static SimpleDateFormat  dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
  public MastiffSerDe() throws SerDeException {
  }

  private void initMastiffSerDeParameters(Configuration job, Properties tbl, String serdeName)
      throws SerDeException, IOException {
    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);
    this.job = job;
    this.tbl = tbl;
  }

  @Override
  public void initialize(Configuration job, Properties tbl) throws SerDeException {

    try {
      initMastiffSerDeParameters(job, tbl, getClass().getName());
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Create the ObjectInspectors from the fields.
    cachedObjectInspector = createMastiffRowObjectInspector(
        serdeParams.getColumnNames(),
        serdeParams.getColumnTypes());

    cachedMastiffRow = new LazyMastiffRow(
        (LazyMastiffRowObjectInspector) cachedObjectInspector);

    LOG.debug("MastiffSerDe initialized with: columnNames="
        + serdeParams.getColumnNames() + " columnTypes="
        + serdeParams.getColumnTypes());
  }

  /**
   * Get the ObjectInspector from serdeParams<br/>
   * Different from HbaseSerDe in field's object inspector,
   * since SegmentFile's fields was serialized from primitive java types,
   * we use PrimitiveJavaObjectInspector instead
   *
   * @param columnNames
   * @param columnTypes
   * @return
   */
  public static ObjectInspector createMastiffRowObjectInspector(List<String> columnNames,
      List<TypeInfo> columnTypes) {
    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        columnTypes.size());
    for (int i = 0; i < columnTypes.size(); i++) {
      TypeInfo ti = columnTypes.get(i);
      if (ti instanceof PrimitiveTypeInfo) {
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) ti;
        columnObjectInspectors.add(PrimitiveObjectInspectorFactory.
            getPrimitiveJavaObjectInspector(pti.getPrimitiveCategory()));
      }
      else {
        columnObjectInspectors.add(PrimitiveObjectInspectorFactory.
            getPrimitiveJavaObjectInspector(PrimitiveCategory.VOID));
      }
    }
    LazyMastiffRowObjectInspector result = new LazyMastiffRowObjectInspector(columnNames,
        columnObjectInspectors);
    return result;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    if (mtbl == null) {
      getMTableDesc(job, tbl);
    }
    if (!(blob instanceof RowWritable)) {
      throw new SerDeException(getClass().toString()
          + ": expects RowWritable!");
    }
    RowWritable rw = (RowWritable) blob;
    cachedMastiffRow.init(rw, mtbl);
    return cachedMastiffRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return RowWritable.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    final List<Object> list = soi.getStructFieldsDataAsList(obj);
    //RowMap rowMap=new RowMap(SerializeUtil.desc.clusterTypes.size(),ETLUtils.getSchema(SerializeUtil.desc.clusterTypes), ETLUtils.getSchema(ETLUtils.getSchema(SerializeUtil.desc.clusterTypes))) ;
    if(isRowMapInit==false){
      rowMap=new RowMap(SerializeUtil.desc.clusterTypes.size(),ETLUtils.getSchema(SerializeUtil.desc.clusterTypes), ETLUtils.getSchema(ETLUtils.getSchema(SerializeUtil.desc.clusterTypes))) ;
      isRowMapInit=true ;
      //}
      // RowMap  rowMap=new RowMap() ;
      // ArrayList  resuse =new   ArrayList<PrimitiveCategory >();
      //   VOID, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, TIMESTAMP, BINARY, UNKNOWN
      for (int i = 0; i <  SerializeUtil.desc.clusterTypes.size(); i++) {
        int j = 0;
        for (int col : SerializeUtil.desc.columnsMapping[i]) {
          final PrimitiveObjectInspector    foi = (PrimitiveObjectInspector) fields.get(col)
              .getFieldObjectInspector();
          switch (foi.getPrimitiveCategory()) {
          case BOOLEAN:
            // row.parseValue(i, ((BooleanObjectInspector) foi).get(list.get(i)) + "");
            rowMap.row[i].setValue(j, ((BooleanObjectInspector) foi).get(list.get(col)));
            //      resuse.add(((BooleanObjectInspector) foi).get(list.get(col)));
            resuse.add(new OIG(){ public Object get(Object x) {return ((BooleanObjectInspector)foi).get(x);} });

            break;
          case BYTE:
            //  byte b = ((ByteObjectInspector) foi).get(list.get(i));

            rowMap.row[i].setValue(j, ((ByteObjectInspector) foi).get(list.get(col)));
            resuse.add(new OIG(){ public Object get(Object x) {return ((ByteObjectInspector)foi).get(x);} });
            break;
          case SHORT:
            //row.parseValue(i, ((ShortObjectInspector) foi).get(list.get(i)) + "");
            rowMap.row[i].setValue(j, ((ShortObjectInspector) foi).get(list.get(col)));
            resuse.add(new OIG(){ public Object get(Object x) {return ((ShortObjectInspector)foi).get(x);} });
            break;
          case INT:
            // row.parseValue(i, ((IntObjectInspector) foi).get(list.get(i)) + "");

            //        Object  tmpObj=list.get(col);
            //        IntObjectInspector tm= (IntObjectInspector) foi ;
            //        Object  obj1=((IntObjectInspector) foi).get(list.get(col));
            //        RowMap mm=rowMap ;
            //        Row  tmpRow=rowMap.row[i];
            //     //   LazyObject laayObj=(LazyObject) obj1;
            //        tmpRow.setValue(j,  obj1) ;
            rowMap.row[i].setValue(j, ((IntObjectInspector) foi).get(list.get(col)));
            resuse.add(new OIG(){ public Object get(Object x) {return ((IntObjectInspector)foi).get(x);} });

            break;
          case LONG:
            // row.parseValue(i, ((LongObjectInspector) foi).get(list.get(i)) + "");
            rowMap.row[i].setValue(j, ((LongObjectInspector) foi).get(list.get(col)));
            resuse.add(new OIG(){ public Object get(Object x) {return ((LongObjectInspector)foi).get(x);} });
            break;
          case FLOAT:
            //   row.parseValue(i, ((FloatObjectInspector) foi).get(list.get(i)) + "");
            rowMap.row[i].setValue(j, ((FloatObjectInspector) foi).get(list.get(col)));
            resuse.add(new OIG(){ public Object get(Object x) {return ((FloatObjectInspector)foi).get(x);} });
            break;
          case DOUBLE:
            // row.parseValue(i, ((DoubleObjectInspector) foi).get(list.get(i)) + "");
            rowMap.row[i].setValue(j, ((DoubleObjectInspector) foi).get(list.get(col)));
            resuse.add(new OIG(){ public Object get(Object x) {return ((DoubleObjectInspector)foi).get(x);} });

            break;
          case STRING:
            //        row.parseValue(i, ((StringObjectInspector) foi).getPrimitiveWritableObject(list.get(i))
            //            .toString());
            rowMap.row[i].setValue(j, ((StringObjectInspector) foi).getPrimitiveWritableObject(list.get(col)).toString());

            resuse.add(new OIG(){ public Object get(Object x) {return ((StringObjectInspector) foi).getPrimitiveWritableObject(x).toString();} });
            break;
          case TIMESTAMP:
            //                        TimestampWritable    time=   ((TimestampObjectInspector) foi).getPrimitiveWritableObject(list.get(col));
            //                        int dj= time.getSeconds();
            //                     long ll=dj
            //                //  Intege m=      Integer.parseInt(dj+"");
            //                   long kk= Long.parseLong(dj+"");
            //            try {
            //              long tmp= ((java.util.Date) dateFormatter.parse(((TimestampObjectInspector) foi).getPrimitiveWritableObject(list.get(col)).toString())).getTime() / 1000;
            //            } catch (ParseException e1) {
            //              // TODO Auto-generated catch block
            //              e1.printStackTrace();
            //            }
            //  Integer.parseInt(((TimestampObjectInspector) foi).getPrimitiveWritableObject(list.get(col)).getSeconds()+"");
            // Long.parseLong(((TimestampObjectInspector) foi).getPrimitiveWritableObject(list.get(col)).getSeconds()+"");
            //  rowMap.row[i].setValue(j,  ((java.util.Date) dateFormatter.parse(((TimestampObjectInspector) foi).getPrimitiveWritableObject(list.get(col)).toString())).getTime() / 1000);
            //     rowMap.row[i].setValue(j,  Long.parseLong(((TimestampObjectInspector) foi).getPrimitiveWritableObject(list.get(col)).getSeconds()+""));
            rowMap.row[i].setValue(j, (long)((TimestampObjectInspector) foi).getPrimitiveWritableObject(list.get(col)).getSeconds());
            resuse.add(new OIG(){ public Object get(Object x) {return (long)((TimestampObjectInspector) foi).getPrimitiveWritableObject(x).getSeconds();} });

            break;
          default :
            try {
              throw new RuntimeException("not supported type");
            } catch (Exception e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }

          j++;
          //     foi=null ;
        }

      }
    }
    else{
      count=0 ;
      for (int i = 0; i <  SerializeUtil.desc.clusterTypes.size(); i++) {
        int j = 0;
        for (int col : SerializeUtil.desc.columnsMapping[i]) {
          PrimitiveObjectInspector    foi = (PrimitiveObjectInspector) fields.get(col).getFieldObjectInspector();
          //          switch (foi.getPrimitiveCategory()){
          //          case STRING:
          //
          //            rowMap.row[i].setValue(j, ((StringObjectInspector) foi).getPrimitiveWritableObject(list.get(col)).toString());
          //            break ;
          //          case TIMESTAMP:
          //            try {
          //              rowMap.row[i].setValue(j,  ((java.util.Date) dateFormatter.parse(((TimestampObjectInspector) foi).getPrimitiveWritableObject(list.get(col)).toString())).getTime() / 1000);
          //            } catch (ParseException e) {
          //              // TODO Auto-generated catch block
          //              e.printStackTrace();
          //            }
          //            break ;
          //
          //          default :
          //            rowMap.row[i].setValue(j, resuse.get(count).get(list.get(col)));
          //            count++;
          //            break ;
          //          }
          rowMap.row[i].setValue(j, resuse.get(count).get(list.get(col)));
          count++;
          j++ ;


        }

      }
    }
    //  soi=null;
    //  fields=null ;
    //  list=null ;

    //  }
    //  soi=null;
    //  fields=null ;
    //  list=null ;


    return rowMap ;
  }
  //   VOID, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, TIMESTAMP, BINARY, UNKNOWN
  //    if (isRowInit == false) {
  //      List<DataType> cols = new ArrayList<DataType>();
  //      for (int i = 0; i < fields.size(); i++) {
  //        PrimitiveObjectInspector foi = (PrimitiveObjectInspector) fields.get(i)
  //            .getFieldObjectInspector();
  //
  //        switch (foi.getPrimitiveCategory()) {
  //        case BOOLEAN:
  //          cols.add(DataType.BOOLEAN);
  //          break;
  //        case BYTE:
  //          cols.add(DataType.BYTE);
  //          break;
  //
  //        case SHORT:
  //          cols.add(DataType.SHORT);
  //          break;
  //        case INT:
  //          cols.add(DataType.INT);
  //          break;
  //        case LONG:
  //          cols.add(DataType.LONG);
  //          break;
  //        case FLOAT:
  //          cols.add(DataType.FLOAT);
  //          break;
  //        case DOUBLE:
  //          cols.add(DataType.DOUBLE);
  //          break;
  //        case STRING:
  //          cols.add(DataType.STRING);
  //          break;
  //        case TIMESTAMP:
  //          cols.add(DataType.LONG);
  //          break;
  //        default:
  //          try {
  //            throw new Exception("not supported type");
  //          } catch (Exception e) {
  //            // TODO Auto-generated catch block
  //            e.printStackTrace();
  //          }
  //        }
  //      }
  //      row = new Row(cols);
  //      isRowInit = true;
  //    }

  //    for (int i = 0; i < fields.size(); i++) {
  //      PrimitiveObjectInspector foi = (PrimitiveObjectInspector) fields.get(i)
  //          .getFieldObjectInspector();
  //        switch (foi.getPrimitiveCategory()) {
  //        case BOOLEAN:
  //         // row.parseValue(i, ((BooleanObjectInspector) foi).get(list.get(i)) + "");
  //          row.setValue(i, ((BooleanObjectInspector) foi).get(list.get(i)));
  //          break;
  //    case BYTE:
  //        //  byte b = ((ByteObjectInspector) foi).get(list.get(i));
  //          row.setValue(i, ((ByteObjectInspector) foi).get(list.get(i)));
  //          break;
  //        case SHORT:
  //          //row.parseValue(i, ((ShortObjectInspector) foi).get(list.get(i)) + "");
  //          row.setValue(i, ((ShortObjectInspector) foi).get(list.get(i)));
  //          break;
  //        case INT:
  //         // row.parseValue(i, ((IntObjectInspector) foi).get(list.get(i)) + "");
  //          row.setValue(i, ((IntObjectInspector) foi).get(list.get(i)));
  //          break;
  //        case LONG:
  //         // row.parseValue(i, ((LongObjectInspector) foi).get(list.get(i)) + "");
  //          row.setValue(i, ((LongObjectInspector) foi).get(list.get(i)));
  //          break;
  //        case FLOAT:
  //       //   row.parseValue(i, ((FloatObjectInspector) foi).get(list.get(i)) + "");
  //          row.setValue(i, ((FloatObjectInspector) foi).get(list.get(i)));
  //          break;
  //        case DOUBLE:
  //         // row.parseValue(i, ((DoubleObjectInspector) foi).get(list.get(i)) + "");
  //          row.setValue(i, ((DoubleObjectInspector) foi).get(list.get(i)));
  //          break;
  //        case STRING:
  //  //        row.parseValue(i, ((StringObjectInspector) foi).getPrimitiveWritableObject(list.get(i))
  //  //            .toString());
  //          row.setValue(i, ((StringObjectInspector) foi).getPrimitiveWritableObject(list.get(i)).toString());
  //          break;
  //        case TIMESTAMP:
  //          try {
  //            dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
  //            java.util.Date date = (java.util.Date) dateFormatter
  //                .parse(((TimestampObjectInspector) foi).getPrimitiveWritableObject(list.get(i))
  //                    .toString());
  //           // long time = date.getTime() / 1000;
  //            row.setValue(i, date.getTime() / 1000);
  //            // row.parseValue(i,time+"");
  //
  //            break;
  //          } catch (ParseException e) {
  //            e.printStackTrace();
  //          }
  //       default :
  //          try {
  //            throw new Exception("not supported type");
  //          } catch (Exception e) {
  //            // TODO Auto-generated catch block
  //            e.printStackTrace();
  //          }
  //        }
  //      }
  //    return row;
  // }


  @Override
  public String toString() {
    return getClass().toString()
        + "["
        + Arrays.asList(serdeParams.getSeparators())
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
        .getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
        .getAllStructFieldTypeInfos() + "]";
  }

  private void getMTableDesc(Configuration job, Properties tbl) {
    String tableName = (String) tbl.get("name");
    try {
      MastiffHandlerUtil.setCFMeta(job, tableName);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    mtbl = MastiffHandlerUtil.getMTableDesc(job);
    MastiffHandlerUtil.getColumnInfos(mtbl, tbl);
    MastiffHandlerUtil.getCFTypes(mtbl);
  }
}
interface OIG {
  Object get(Object x);
}
