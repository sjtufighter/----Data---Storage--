package org.apache.hadoop.hive.mastiff;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.ColumnDesc;
import org.apache.hadoop.hive.mastiff.MastiffHandlerUtil.MTableDesc;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import cn.ac.ncic.mastiff.ValPair;

public abstract class MastiffEvaluator {
  MTableDesc tblDesc;
  ObjectInspector cachedMastiffRowOI;
  public static final byte ONE = 1;
  public static final byte ZERO = 0;
}

class SingleColEvaluator extends MastiffEvaluator {
  private HashMap<ExprNodeDesc, RowWritable> cachedRowWritables;
  private HashMap<ExprNodeDesc, LazyMastiffRow> cachedLMR;
  private HashMap<ExprNodeDesc, ExprNodeEvaluator> cachedEvaluators;
  private HashMap<ExprNodeEvaluator, PrimitiveObjectInspector> cachedConditionOI;

  public SingleColEvaluator(MTableDesc tblDesc) {
    init(tblDesc);
  }

  public void init(MTableDesc tblDesc) {
    this.tblDesc = tblDesc;
    cachedMastiffRowOI = MastiffSerDe.createMastiffRowObjectInspector(
        Arrays.asList(tblDesc.columnNames), Arrays.asList(tblDesc.columnTypes));
    cachedRowWritables = new HashMap<ExprNodeDesc, RowWritable>();
    cachedLMR = new HashMap<ExprNodeDesc, LazyMastiffRow>();
    cachedEvaluators = new HashMap<ExprNodeDesc, ExprNodeEvaluator>();
    cachedConditionOI = new HashMap<ExprNodeEvaluator, PrimitiveObjectInspector>();
  }

  public Boolean evaluate(MTableDesc tblDesc, ExprNodeDesc filter, List<ValPair> vps) {
    if (tblDesc != this.tblDesc && !tblDesc.equals(this.tblDesc)) {
      init(tblDesc);
    }
    RowWritable curRw;
    LazyMastiffRow curLrw;
    ExprNodeEvaluator curConditionEvaluator = null;
    PrimitiveObjectInspector curConditionOI = null;
    if (!cachedEvaluators.containsKey(filter)) {
      curRw = new RowWritable();
      int colId = MastiffHandlerUtil.getColIdFromFilter(tblDesc, filter).get(0);
      ColumnDesc cd = MastiffHandlerUtil.getCF(tblDesc, colId);
      curRw.set(ONE, vps, Arrays.asList(cd.cf), Arrays.asList(cd.idxInTbl));
      cachedRowWritables.put(filter, curRw);

      curLrw = new LazyMastiffRow((LazyMastiffRowObjectInspector) cachedMastiffRowOI);
      curLrw.init(curRw, tblDesc);
      cachedLMR.put(filter, curLrw);
      try {
        curConditionEvaluator = MastiffExprEvaluatorFactory.get(filter);
      } catch (HiveException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      cachedEvaluators.put(filter, curConditionEvaluator);

      try {
        curConditionOI = (PrimitiveObjectInspector) curConditionEvaluator
            .initialize(cachedMastiffRowOI);
        cachedConditionOI.put(curConditionEvaluator, curConditionOI);
      } catch (HiveException e) {
        e.printStackTrace();
      }
    } else {
      curRw = cachedRowWritables.get(filter);
      curLrw = cachedLMR.get(filter);
      curConditionEvaluator = cachedEvaluators.get(filter);
      curConditionOI = cachedConditionOI.get(curConditionEvaluator);
      curRw.set(ZERO, vps, null, null);
      curLrw.init(curRw, tblDesc);
    }

    Object cond;
    Boolean ret = false;
    try {
      cond = curConditionEvaluator.evaluate(curLrw);
      ret = (Boolean) curConditionOI.getPrimitiveJavaObject(cond);
    } catch (HiveException e) {
      e.printStackTrace();
    }

    return ret;
  }
}

class RowEvaluator extends MastiffEvaluator {
  private RowWritable rw;
  private LazyMastiffRow lrw;
  private ExprNodeEvaluator evaluator = null;
  private PrimitiveObjectInspector conditionOI;

  public RowEvaluator(MTableDesc tblDesc, ExprNodeDesc filter) {
    this.tblDesc = tblDesc;
    this.cachedMastiffRowOI = MastiffSerDe.createMastiffRowObjectInspector(
        Arrays.asList(tblDesc.columnNames), Arrays.asList(tblDesc.columnTypes));
    try {
      this.evaluator = MastiffExprEvaluatorFactory.get(filter);
    } catch (HiveException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    try {
      this.conditionOI = (PrimitiveObjectInspector) this.evaluator
          .initialize(cachedMastiffRowOI);
    } catch (HiveException e) {
      e.printStackTrace();
    }
  }

  public Boolean evaluate(List<ValPair> vps, List<Integer> filterCfs, List<Integer> filterColumns) {
    if (rw == null) {
      rw = new RowWritable();
      rw.set(ONE, vps, filterCfs, filterColumns);
      lrw = new LazyMastiffRow((LazyMastiffRowObjectInspector) cachedMastiffRowOI);
    } else {
      rw.set(ZERO, vps, null, null);
    }
    lrw.init(rw, tblDesc);
    Object cond;
    Boolean ret = false;
    try {
      cond = evaluator.evaluate(lrw);
      ret = (Boolean) conditionOI.getPrimitiveJavaObject(cond);
    } catch (HiveException e) {
      e.printStackTrace();
    }
    return ret;
  }

}
