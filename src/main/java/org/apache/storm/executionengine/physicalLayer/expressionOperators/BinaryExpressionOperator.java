package org.apache.storm.executionengine.physicalLayer.expressionOperators;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * A base class for all Binary expression operators. Supports the lhs and rhs
 * operators which are used to fetch the inputs and apply the appropriate
 * operation with the appropriate type.
 * 
 */
public abstract class BinaryExpressionOperator extends ExpressionOperator {
  private static final long serialVersionUID = 1L;

  protected ExpressionOperator lhs;
  protected ExpressionOperator rhs;
  private transient List<ExpressionOperator> child;

  public BinaryExpressionOperator(OperatorKey k) {
    this(k, -1);
  }

  public BinaryExpressionOperator(OperatorKey k, int rp) {
    super(k, rp);
  }

  public ExpressionOperator getLhs() {
    return lhs;
  }

  /**
   * Get the child expressions of this expression
   */
  public List<ExpressionOperator> getChildExpressions() {
    if (child == null) {
      child = new ArrayList<ExpressionOperator>();
      child.add(lhs);
      child.add(rhs);
    }
    return child;
  }

  @Override
  public boolean supportsMultipleInputs() {
    return true;
  }

  public void setLhs(ExpressionOperator lhs) {
    this.lhs = lhs;
  }

  public ExpressionOperator getRhs() {
    return rhs;
  }

  public void setRhs(ExpressionOperator rhs) {
    this.rhs = rhs;
  }

  protected void cloneHelper(BinaryExpressionOperator op) {
    // Don't clone these, as they are just references to things already in
    // the plan.
    lhs = op.lhs;
    rhs = op.rhs;
    super.cloneHelper(op);
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    return null;
  }
}
