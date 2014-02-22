package org.apache.hadoop.hive.mastiff;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.mastiff.Cell.ClmLocation;
import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.ColumnDesc;
import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.MTableDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;

import cn.ac.ncic.mastiff.ValPair;
import cn.ac.ncic.mastiff.utils.Bytes;

/**
 * Lazy object for storing the selected cfs' data
 *
 */
public class LazyMastiffRow extends LazyMastiffRowBase{

  private HashMap<Integer, Cell> cells;
  private RowWritable result; //value to be deserialized
  private ArrayList<Object> cachedList;
  private List<Integer> validCfs; //selected column familys
  private List<Integer> validColumns; //selected columns
  private MTableDesc tblDesc;
  private List<ObjectInspector> fieldsOI;

  private Object[] fields; //store java primitives deserialized from RowWritable

  public LazyMastiffRow(LazyMastiffRowObjectInspector oi) {
    super(oi);
  }

  public void init(RowWritable rw, MTableDesc tblDesc) {
    result = rw;
    this.tblDesc = tblDesc;
    cells = new HashMap<Integer, Cell>();
    if(result.getIsFirst() == 1) {
      validCfs = Arrays.asList(rw.getValidCfs());
      validColumns = Arrays.asList(rw.getValidColumns());
    }
    for (Integer cfIdx : validCfs) {
      Cell curCell = new Cell();
      curCell.init(Arrays.asList(tblDesc.clusterTypes[cfIdx]));
      cells.put(cfIdx, curCell);
    }
    setParsed(false);
  }

  private void parse() {
    if(getFields() == null) {
      List<? extends StructField> fieldRefs =
          ((StructObjectInspector)getInspector()).getAllStructFieldRefs();
      Object [] flds = new Object[fieldRefs.size()];
      List<ObjectInspector> fieldsOI = new ArrayList<ObjectInspector>();

      for(int i = 0; i < flds.length; i++) {
        fieldsOI.add(fieldRefs.get(i).getFieldObjectInspector());
      }
      setFields(flds);
      setFieldsOI(fieldsOI);
      setFieldInited(new boolean[flds.length]);
    }
    Arrays.fill(getFieldInited(), false);
    setParsed(true);
  }

  public Object getField(int fieldID) {
    if (!getParsed()) {
      parse();
    }
    return uncheckedGetField(fieldID);
  }

  private Object uncheckedGetField(int fieldID) {
    Object [] fields = getFields();
    boolean [] fieldsInited = getFieldInited();

    if(!fieldsInited[fieldID]) {

      ColumnDesc cdesc = MastiffHandlerUtil.getCF(tblDesc, fieldID);
      if(validColumns.contains(fieldID)) {
        int idx = validCfs.indexOf(cdesc.cf);
        ValPair curvp = result.getVps()[idx];
        Cell curcell = cells.get(cdesc.cf);
        curcell.getFieldLocation(curvp);
        ClmLocation curField = curcell.getLocations().get(cdesc.idxInCf);

        fields[fieldID] = getJavaPrimitiveObject(fieldID, curvp, curField);
      }
      else { //fieldID not in selected columns
        return null;
      }
    }
    fieldsInited[fieldID] = true;
    return fields[fieldID];
  }

  public ArrayList<Object> getFieldsAsList() {
    if(!getParsed()) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < getFields().length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }

  @Override
  public Object getObject() {
    return this;
  }

  private void setFieldsOI(List<ObjectInspector> fieldsOI) {
    this.fieldsOI = fieldsOI;
  }

  /**
   * Get Java primitives from {@link ValPair}
   * @param fieldID
   * @param vp
   * @param loc Offset and length of current field
   * @return Java primitive representation of fieldID
   */
  private Object getJavaPrimitiveObject(int fieldID, ValPair vp, ClmLocation loc) {
    ObjectInspector oi = fieldsOI.get(fieldID);
    if(oi.getCategory() == Category.PRIMITIVE) {
      AbstractPrimitiveObjectInspector apoi = (AbstractPrimitiveObjectInspector)oi;
      PrimitiveCategory pc = apoi.getPrimitiveCategory();
      int offset = vp.offset + loc.offset;
      int length = loc.length;
      switch(pc) {
      case BOOLEAN:
        byte bl = vp.data[offset];
        if(bl == 0) {
          return false;
        } else if(bl == 1) {
          return true;
        }
      case BYTE:
        return vp.data[offset];
      case SHORT:
        return Bytes.toShort(vp.data, offset);
      case INT:
        return Bytes.toInt(vp.data, offset);
      case LONG:
        return Bytes.toLong(vp.data, offset);
      case FLOAT:
        return Bytes.toFloat(vp.data, offset);
      case DOUBLE:
        return Bytes.toDouble(vp.data, offset);
      case STRING:
        return Bytes.toString(vp.data, offset, length);
      case TIMESTAMP:
        long time = Bytes.toLong(vp.data, offset);
        return new Timestamp(time * 1000);
      case BINARY:
      case DECIMAL:
      case VOID:
      case UNKNOWN:
        return new Object();
      }
    }
    return new Object();
  }
}
