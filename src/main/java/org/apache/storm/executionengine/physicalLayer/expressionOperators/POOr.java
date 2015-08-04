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
 * Boolean or operator.
 */

public class POOr extends BinaryComparisonOperator {

  /**
     * 
     */
  private static final long serialVersionUID = 1L;

  public POOr(OperatorKey k) {
    this(k, -1);
  }

  public POOr(OperatorKey k, int rp) {
    super(k, rp);
    resultType = DataType.BOOLEAN;
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitOr(this);
  }

  @Override
  public String name() {
    return "Or" + "[" + DataType.findTypeName(resultType) + "]" + " - "
        + mKey.toString();
  }

  @Override
  public Result getNextBoolean() throws ExecException {
    Result r = accumChild(null, DataType.BOOLEAN);
    if (r != null) {
      return r;
    }

    Result left;
    left = lhs.getNextBoolean();
    // pass on ERROR and EOP
    if (left.returnStatus != POStatus.STATUS_OK
        && left.returnStatus != POStatus.STATUS_NULL) {
      return left;
    }

    // truth table for OR
    // t = true, n = null, f = false
    // OR t n f
    // 1) t t t t
    // 2) n t n n
    // 3) f t n f

    // Short circuit. if lhs is true, return true - ROW 1 above is handled
    // with this
    boolean returnLeft = false;
    if (left.result != null && ((Boolean) left.result).booleanValue()) {
      if (illustrator == null)
        return left;

      illustratorMarkup(null, left.result, 0);
      returnLeft = true;
      ;
    }

    Result right = rhs.getNextBoolean();
    if (returnLeft)
      return left;

    // pass on ERROR and EOP
    if (right.returnStatus != POStatus.STATUS_OK
        && right.returnStatus != POStatus.STATUS_NULL) {
      return right;
    }

    // if the lhs is null and rhs is false - return null , in all other
    // cases, we can
    // just return rhs - ROW 2 and ROW 3 above
    if (left.result == null && right.result != null
        && !((Boolean) right.result).booleanValue()) {
      return left;
    }

    // No matter what, what we get from the right side is what we'll
    // return, null, true, or false.
    if (right.result != null)
      illustratorMarkup(null, right.result, (Boolean) right.result ? 0 : 1);
    return right;
  }

  @Override
  public POOr clone() throws CloneNotSupportedException {
    POOr clone = new POOr(new OperatorKey(mKey.scope, NodeIdGenerator
        .getGenerator().getNextNodeId(mKey.scope)));
    clone.cloneHelper(this);
    return clone;
  }
}
