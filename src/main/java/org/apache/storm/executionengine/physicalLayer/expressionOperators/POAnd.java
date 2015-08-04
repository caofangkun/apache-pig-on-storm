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
 * Boolean and operator.
 */

public class POAnd extends BinaryComparisonOperator {

  /**
     * 
     */
  private static final long serialVersionUID = 1L;

  public POAnd(OperatorKey k) {
    this(k, -1);
  }

  public POAnd(OperatorKey k, int rp) {
    super(k, rp);
    resultType = DataType.BOOLEAN;
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitAnd(this);
  }

  @Override
  public String name() {
    return "And" + "[" + DataType.findTypeName(resultType) + "]" + " - "
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

    // truth table for AND
    // t = true, n = null, f = false
    // AND t n f
    // 1) t t n f
    // 2) n n n f
    // 3) f f f f

    // Short circuit - if lhs is false, return false; ROW 3 above is handled
    // with this
    boolean returnLeft = false;
    if (left.result != null && !(((Boolean) left.result).booleanValue())) {
      if (illustrator == null) {
        return left;
      }
      illustratorMarkup(null, left.result, 1);
      returnLeft = true;
    }

    Result right = rhs.getNextBoolean();
    if (returnLeft) {
      return left;
    }

    // pass on ERROR and EOP
    if (right.returnStatus != POStatus.STATUS_OK
        && right.returnStatus != POStatus.STATUS_NULL) {
      return right;
    }

    // if the lhs is null and rhs is true - return null, in all other cases
    // we can just return the rhs - ROW 1 and ROW 2 of table above
    if (left.result == null && right.result != null
        && ((Boolean) right.result).booleanValue()) {
      return left;
    }

    // No matter what, what we get from the right side is what we'll
    // return, null, true, or false.
    if (right.result != null)
      illustratorMarkup(null, right.result, (Boolean) right.result ? 0 : 1);
    return right;
  }

  @Override
  public POAnd clone() throws CloneNotSupportedException {
    POAnd clone = new POAnd(new OperatorKey(mKey.scope, NodeIdGenerator
        .getGenerator().getNextNodeId(mKey.scope)));
    clone.cloneHelper(this);
    return clone;
  }
}
