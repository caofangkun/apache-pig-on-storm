package org.apache.storm.executionengine.physicalLayer.expressionOperators;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.regex.RegexImpl;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.regex.RegexInit;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class PORegexp extends BinaryComparisonOperator {

  private static final long serialVersionUID = 1L;

  private RegexImpl impl = null;

  public PORegexp(OperatorKey k) {
    this(k, -1);
  }

  public PORegexp(OperatorKey k, int rp) {
    super(k, rp);
    resultType = DataType.BOOLEAN;
    // We set impl RegexInit.
    // RegexInit decides what plan to choose after looking
    // at first tuple. And resets this.impl to new implementation
    this.impl = new RegexInit(this);
  }

  public void setImplementation(RegexImpl impl) {
    this.impl = impl;
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitRegexp(this);
  }

  @Override
  public String name() {
    return "Matches - " + mKey.toString();
  }

  public void setConstExpr(boolean rhsConstant) {
    ((RegexInit) this.impl).setConstExpr(rhsConstant);
  }

  @Override
  public Result getNextBoolean() throws ExecException {
    Result r = accumChild(null, DataType.CHARARRAY);
    if (r != null) {
      return r;
    }

    Result left, right;

    left = lhs.getNextString();
    right = rhs.getNextString();

    if (left.returnStatus != POStatus.STATUS_OK || left.result == null)
      return left;
    if (right.returnStatus != POStatus.STATUS_OK || right.result == null)
      return right;

    if (impl.match((String) (left.result), (String) (right.result))) {
      left.result = Boolean.TRUE;
    } else {
      left.result = Boolean.FALSE;
    }
    illustratorMarkup(null, left.result, (Boolean) left.result ? 0 : 1);
    return left;
  }

  @Override
  public PORegexp clone() throws CloneNotSupportedException {
    PORegexp clone = new PORegexp(new OperatorKey(mKey.scope, NodeIdGenerator
        .getGenerator().getNextNodeId(mKey.scope)));
    clone.cloneHelper(this);
    return clone;
  }
}
