package org.apache.hadoop.hive.mastiff;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * 
 * LazyMastiffRowObjectInspector works on data that is stored in
 * LazyMastiffRow
 * <p>
 * The names of the struct fields and the internal structure of the struct fields are specified in
 * the {@link #fields} of the LazyMastiffRowObjectInspector. <br/>
 * 
 * Adapted from {@link ColumnarSturctObjectInspector}
 * 
 */
public class LazyMastiffRowObjectInspector extends StructObjectInspector {

  public static final Log LOG = LogFactory
      .getLog(LazyMastiffRowObjectInspector.class.getName());

  protected static class MyField implements StructField {
    protected int fieldID;
    protected String fieldName;
    protected ObjectInspector fieldObjectInspector;
    protected String fieldComment;

    public MyField(int fieldID, String fieldName,
        ObjectInspector fieldObjectInspector) {
      this.fieldID = fieldID;
      this.fieldName = fieldName.toLowerCase();
      this.fieldObjectInspector = fieldObjectInspector;
    }

    public MyField(int fieldID, String fieldName,
        ObjectInspector fieldObjectInspector, String fieldComment) {
      this(fieldID, fieldName, fieldObjectInspector);
      this.fieldComment = fieldComment;
    }

    public int getFieldID() {
      return fieldID;
    }

    public String getFieldName() {
      return fieldName;
    }

    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }

    public String getFieldComment() {
      return fieldComment;
    }

    @Override
    public String toString() {
      return "" + fieldID + ":" + fieldName;
    }
  }

  protected List<MyField> fields;

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  /**
   *
   */
  public LazyMastiffRowObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors) {
    init(structFieldNames, structFieldObjectInspectors, null);
  }

  public LazyMastiffRowObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments) {
    init(structFieldNames, structFieldObjectInspectors, structFieldComments);
  }

  protected void init(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments) {
    assert (structFieldNames.size() == structFieldObjectInspectors.size());
    assert (structFieldComments == null || (structFieldNames.size() == structFieldComments.size()));

    fields = new ArrayList<MyField>(structFieldNames.size());
    for (int i = 0; i < structFieldNames.size(); i++) {
      fields.add(new MyField(i, structFieldNames.get(i),
          structFieldObjectInspectors.get(i),
          structFieldComments == null ? null : structFieldComments.get(i)));
    }
  }

  protected LazyMastiffRowObjectInspector(List<StructField> fields) {
    init(fields);
  }

  protected void init(List<StructField> fields) {
    this.fields = new ArrayList<MyField>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      this.fields.add(new MyField(i, fields.get(i).getFieldName(), fields
          .get(i).getFieldObjectInspector(), fields.get(i).getFieldComment()));
    }
  }

  @Override
  public final Category getCategory() {
    return Category.STRUCT;
  }

  // Without Data
  @Override
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  // With Data
  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    LazyMastiffRow struct = (LazyMastiffRow) data;
    MyField f = (MyField) fieldRef;

    int fieldID = f.getFieldID();
    assert (fieldID >= 0 && fieldID < fields.size());

    return struct.getField(fieldID);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    LazyMastiffRow struct = (LazyMastiffRow) data;
    return struct.getFieldsAsList();
  }

}
