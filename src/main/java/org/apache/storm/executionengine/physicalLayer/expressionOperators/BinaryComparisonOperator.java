package org.apache.storm.executionengine.physicalLayer.expressionOperators;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * This is a base class for all binary comparison operators. Supports the use of
 * operand type instead of result type as the result type is always boolean.
 * 
 * All comparison operators fetch the lhs and rhs operands and compare them for
 * each type using different comparison methods based on what comparison is
 * being implemented.
 * 
 */
public abstract class BinaryComparisonOperator extends BinaryExpressionOperator
    implements ComparisonOperator {
  private static final long serialVersionUID = -1711987060802103390L;

  protected byte operandType;

  public BinaryComparisonOperator(OperatorKey k) {
    this(k, -1);
  }

  public BinaryComparisonOperator(OperatorKey k, int rp) {
    super(k, rp);
  }

  public byte getOperandType() {
    return operandType;
  }

  public void setOperandType(byte operandType) {
    this.operandType = operandType;
  }

  protected void cloneHelper(BinaryComparisonOperator op) {
    operandType = op.operandType;
    super.cloneHelper(op);
  }

  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    if (illustrator != null) {
      illustrator.setSubExpResult(eqClassIndex == 0);
    }
    return null;
  }
}
