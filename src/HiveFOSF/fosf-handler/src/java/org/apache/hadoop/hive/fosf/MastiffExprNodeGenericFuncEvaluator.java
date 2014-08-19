package org.apache.hadoop.hive.mastiff;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.ExprNodeGenericFuncEvaluator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

public class MastiffExprNodeGenericFuncEvaluator extends ExprNodeEvaluator<ExprNodeGenericFuncDesc> {

  private static final Log LOG = LogFactory
      .getLog(ExprNodeGenericFuncEvaluator.class.getName());

  transient GenericUDF genericUDF;
  transient Object rowObject;
  transient ExprNodeEvaluator[] children;
  transient GenericUDF.DeferredObject[] deferredChildren;

  /**
   * Class to allow deferred evaluation for GenericUDF.
   */
  class DeferredExprObject implements GenericUDF.DeferredObject {

    private final ExprNodeEvaluator eval;

    private transient boolean evaluated;
    private transient int version;
    private transient Object obj;

    DeferredExprObject(ExprNodeEvaluator eval) {
      this.eval = eval;
    }

    @Override
    public void prepare(int version) throws HiveException {
      this.version = version;
      this.evaluated = false;
    }

    public Object get() throws HiveException {
      if (!evaluated) {
        obj = eval.evaluate(rowObject, version);
        evaluated = true;
      }
      return obj;
    }
  }

  public MastiffExprNodeGenericFuncEvaluator(ExprNodeGenericFuncDesc expr) throws HiveException {
    super(expr);
    children = new ExprNodeEvaluator[expr.getChildExprs().size()];
    for (int i = 0; i < children.length; i++) {
      ExprNodeDesc child = expr.getChildExprs().get(i);
      ExprNodeEvaluator nodeEvaluator = ExprNodeEvaluatorFactory.get(child);
      children[i] = nodeEvaluator;
    }
    genericUDF = expr.getGenericUDF();
    if (genericUDF instanceof GenericUDFCase || genericUDF instanceof GenericUDFWhen) {
      throw new HiveException("Stateful expressions cannot be used inside of CASE");
    }
  }

  @Override
  public ObjectInspector initialize(ObjectInspector rowInspector) throws HiveException {
    deferredChildren = new GenericUDF.DeferredObject[children.length];
    for (int i = 0; i < deferredChildren.length; i++) {
      deferredChildren[i] = new DeferredExprObject(children[i]);
    }
    // Initialize all children first
    ObjectInspector[] childrenOIs = new ObjectInspector[children.length];
    for (int i = 0; i < children.length; i++) {
      childrenOIs[i] = children[i].initialize(rowInspector);
    }
    MapredContext context = MapredContext.get();
    if (context != null) {
      context.setup(genericUDF);
    }
    return outputOI = genericUDF.initializeAndFoldConstants(childrenOIs);
  }

  @Override
  public boolean isDeterministic() {
    return true;
  }

  @Override
  public ExprNodeEvaluator[] getChildren() {
    return children;
  }

  @Override
  public boolean isStateful() {
    return false;
  }

  @Override
  protected Object _evaluate(Object row, int version) throws HiveException {
    rowObject = row;
    if (ObjectInspectorUtils.isConstantObjectInspector(outputOI) &&
        isDeterministic()) {
      // The output of this UDF is constant, so don't even bother evaluating.
      return ((ConstantObjectInspector) outputOI).getWritableConstantValue();
    }
    for (int i = 0; i < deferredChildren.length; i++) {
      deferredChildren[i].prepare(version);
    }
    return genericUDF.evaluate(deferredChildren);
  }

  /**
   * If the genericUDF is a base comparison, it returns an integer based on the result of comparing
   * the two sides of the UDF, like the compareTo method in Comparable.
   * 
   * If the genericUDF is not a base comparison, or there is an error executing the comparison, it
   * returns null.
   * 
   * @param row
   * @return the compare results
   * @throws HiveException
   */
  public Integer compare(Object row) throws HiveException {
    if (!expr.isSortedExpr() || !(genericUDF instanceof GenericUDFBaseCompare)) {
      for (ExprNodeEvaluator evaluator : children) {
        if (evaluator instanceof ExprNodeGenericFuncEvaluator) {
          Integer comparison = ((ExprNodeGenericFuncEvaluator) evaluator).compare(row);
          if (comparison != null) {
            return comparison;
          }
        }
      }
      return null;
    }

    rowObject = row;
    for (int i = 0; i < deferredChildren.length; i++) {
      deferredChildren[i].prepare(-1);
    }
    return ((GenericUDFBaseCompare) genericUDF).compare(deferredChildren);
  }
}
