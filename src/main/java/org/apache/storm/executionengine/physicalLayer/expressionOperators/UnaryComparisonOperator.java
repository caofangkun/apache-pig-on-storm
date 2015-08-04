package org.apache.storm.executionengine.physicalLayer.expressionOperators;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * This is a base class for all unary comparison operators. Supports the use of
 * operand type instead of result type as the result type is always boolean.
 * 
 */
public abstract class UnaryComparisonOperator extends UnaryExpressionOperator
    implements ComparisonOperator {
  // The result type for comparison operators is always
  // Boolean. So the plans evaluating these should consider
  // the type of the operands instead of the result.
  // The result will be comunicated using the Status object.
  // This is a slight abuse of the status object.
  protected byte operandType;

  public UnaryComparisonOperator(OperatorKey k) {
    this(k, -1);
  }

  public UnaryComparisonOperator(OperatorKey k, int rp) {
    super(k, rp);
  }

  public byte getOperandType() {
    return operandType;
  }

  public void setOperandType(byte operandType) {
    this.operandType = operandType;
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    if (illustrator != null) {
      illustrator.setSubExpResult(eqClassIndex == 0);
    }
    return null;
  }
}
