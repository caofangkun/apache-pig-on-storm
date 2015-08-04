package org.apache.storm.executionengine.physicalLayer.expressionOperators;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.Result;

/**
 * This is an interface for all comparison operators. Supports the use of
 * operand type instead of result type as the result type is always boolean.
 * 
 */
public interface ComparisonOperator {

  /**
   * Determine the type of the operand(s) of this comparator.
   * 
   * @return type, as a byte (using DataType types).
   */
  byte getOperandType();

  /**
   * Set the type of the operand(s) of this comparator.
   * 
   * @param operandType
   *          Type of the operand(s), as a byte (using DataType types).
   */
  void setOperandType(byte operandType);

  // Stupid java doesn't allow multiple inheritence, so I have to duplicate
  // all the getNext functions here so that comparitors can have them.
  public Result getNextInteger() throws ExecException;

  public Result getNextLong() throws ExecException;

  public Result getNextDouble() throws ExecException;

  public Result getNextFloat() throws ExecException;

  public Result getNextString() throws ExecException;

  public Result getNextDataByteArray() throws ExecException;

  public Result getNextMap() throws ExecException;

  public Result getNextBoolean() throws ExecException;

  public Result getNextDateTime() throws ExecException;

  public Result getNextTuple() throws ExecException;

  public Result getNextDataBag() throws ExecException;

}
