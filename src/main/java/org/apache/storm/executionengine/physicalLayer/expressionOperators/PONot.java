package org.apache.storm.executionengine.physicalLayer.expressionOperators;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Boolean not operator.
 */

public class PONot extends UnaryComparisonOperator {

  /**
     * 
     */
  private static final long serialVersionUID = 1L;

  private Result trueRes, falseRes;

  public PONot(OperatorKey k) {
    this(k, -1);
  }

  public PONot(OperatorKey k, int rp) {
    super(k, rp);
    resultType = DataType.BOOLEAN;
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitNot(this);
  }

  @Override
  public String name() {
    return "Not" + "[" + DataType.findTypeName(resultType) + "]" + " - "
        + mKey.toString();
  }

  @Override
  public Result getNextBoolean() throws ExecException {
    if (trueRes == null) {
      trueRes = new Result();
      trueRes.returnStatus = POStatus.STATUS_OK;
      trueRes.result = true;
      falseRes = new Result();
      falseRes.returnStatus = POStatus.STATUS_OK;
      falseRes.result = Boolean.valueOf(false);
    }
    res = expr.getNextBoolean();
    if (res.returnStatus != POStatus.STATUS_OK || res.result == null) {
      return res;
    }
    if (((Boolean) res.result).booleanValue()) {
      illustratorMarkup(null, falseRes.result, 1);
      return falseRes;
    } else {
      illustratorMarkup(null, trueRes.result, 0);
      return trueRes;
    }
  }

  @Override
  public PONot clone() throws CloneNotSupportedException {
    PONot clone = new PONot(new OperatorKey(mKey.scope, NodeIdGenerator
        .getGenerator().getNextNodeId(mKey.scope)));
    clone.cloneHelper(this);
    return clone;
  }
}
