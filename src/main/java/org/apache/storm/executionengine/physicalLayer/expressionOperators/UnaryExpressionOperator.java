package org.apache.storm.executionengine.physicalLayer.expressionOperators;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.OperatorKey;

public abstract class UnaryExpressionOperator extends ExpressionOperator {

  ExpressionOperator expr;
  private transient List<ExpressionOperator> child;

  public UnaryExpressionOperator(OperatorKey k, int rp) {
    super(k, rp);

  }

  public UnaryExpressionOperator(OperatorKey k) {
    super(k);

  }

  @Override
  public boolean supportsMultipleInputs() {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * Set the contained expression to the be the input value.
   */
  public void setInputAsExpr(PhysicalPlan plan) {
    expr = (ExpressionOperator) plan.getPredecessors(this).get(0);
  }

  /**
   * Set the contained expression explicitly. This is mostly for testing.
   * 
   * @param e
   *          Expression to contain.
   */
  public void setExpr(ExpressionOperator e) {
    expr = e;
  }

  /**
   * Get the contained expression.
   * 
   * @return contained expression.
   */
  public ExpressionOperator getExpr() {
    return expr;
  }

  protected void cloneHelper(UnaryExpressionOperator op) {
    // Don't clone this, as it is just a reference to something already in
    // the plan.
    expr = op.expr;
    resultType = op.getResultType();
  }

  /**
   * Get child expression of this expression
   */
  @Override
  public List<ExpressionOperator> getChildExpressions() {
    if (child == null) {
      child = new ArrayList<ExpressionOperator>();
      child.add(expr);
    }

    return child;
  }

}
