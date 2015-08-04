package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;

public class POLimit extends PhysicalOperator {
  /**
     * 
     */
  private static final long serialVersionUID = 1L;

  // Counts for outputs processed
  private long soFar = 0;

  // Number of limited outputs
  long mLimit;

  // The expression plan
  PhysicalPlan expressionPlan;

  public POLimit(OperatorKey k) {
    this(k, -1, null);
  }

  public POLimit(OperatorKey k, int rp) {
    this(k, rp, null);
  }

  public POLimit(OperatorKey k, List<PhysicalOperator> inputs) {
    this(k, -1, inputs);
  }

  public POLimit(OperatorKey k, int rp, List<PhysicalOperator> inputs) {
    super(k, rp, inputs);
  }

  public void setLimit(long limit) {
    mLimit = limit;
  }

  public long getLimit() {
    return mLimit;
  }

  public PhysicalPlan getLimitPlan() {
    return expressionPlan;
  }

  public void setLimitPlan(PhysicalPlan expressionPlan) {
    this.expressionPlan = expressionPlan;
  }

  @Override
  public Result processDataBags(DataBag... bags) throws ExecException {
    DataBag bag = bags[0];
    Result r = new Result();
    if (bag == null) {
      r.returnStatus = POStatus.STATUS_NULL;
      return r;
    }
    // if it is the first time, evaluate the expression. Otherwise reuse the
    // computed value.
    if (this.getLimit() < 0 && expressionPlan != null) {
      PhysicalOperator expression = expressionPlan.getLeaves().get(0);
      long variableLimit;
      Result returnValue;
      switch (expression.getResultType()) {
      case DataType.LONG:
        returnValue = expression.getNextLong();
        if (returnValue.returnStatus != POStatus.STATUS_OK
            || returnValue.result == null)
          throw new RuntimeException("Unable to evaluate Limit expression: "
              + returnValue);
        variableLimit = (Long) returnValue.result;
        break;
      case DataType.INTEGER:
        returnValue = expression.getNextInteger();
        if (returnValue.returnStatus != POStatus.STATUS_OK
            || returnValue.result == null)
          throw new RuntimeException("Unable to evaluate Limit expression: "
              + returnValue);
        variableLimit = (Integer) returnValue.result;
        break;
      default:
        throw new RuntimeException("Limit requires an integer parameter");
      }
      if (variableLimit <= 0)
        throw new RuntimeException(
            "Limit requires a positive integer parameter");
      this.setLimit(variableLimit);
    }
    //
    Iterator<Tuple> it = bag.iterator();
    List<Tuple> tuples = new ArrayList<Tuple>();

    while (it.hasNext()) {
      Tuple t = it.next();
      illustratorMarkup(t, null, 0);
      tuples.add(t);
      
      soFar++;
      // illustrator ignore LIMIT before the post processing
      if ((illustrator == null || illustrator.getOriginalLimit() != -1)
          && soFar >= mLimit)
        break;
      
    }

    if (tuples.size() == 0) {
      r.returnStatus = POStatus.STATUS_NULL;
      r.result = null;
    } else {
      r.returnStatus = POStatus.STATUS_OK;
      r.result = new NonSpillableDataBag(tuples);
    }

    return r;
  }

  /**
   * Counts the number of tuples processed into static variable soFar, if the
   * number of tuples processed reach the limit, return EOP; Otherwise, return
   * the tuple
   */
  @Override
  public Result getNextTuple() throws ExecException {
    // if it is the first time, evaluate the expression. Otherwise reuse the
    // computed value.
    if (this.getLimit() < 0 && expressionPlan != null) {
      PhysicalOperator expression = expressionPlan.getLeaves().get(0);
      long variableLimit;
      Result returnValue;
      switch (expression.getResultType()) {
      case DataType.LONG:
        returnValue = expression.getNextLong();
        if (returnValue.returnStatus != POStatus.STATUS_OK
            || returnValue.result == null)
          throw new RuntimeException("Unable to evaluate Limit expression: "
              + returnValue);
        variableLimit = (Long) returnValue.result;
        break;
      case DataType.INTEGER:
        returnValue = expression.getNextInteger();
        if (returnValue.returnStatus != POStatus.STATUS_OK
            || returnValue.result == null)
          throw new RuntimeException("Unable to evaluate Limit expression: "
              + returnValue);
        variableLimit = (Integer) returnValue.result;
        break;
      default:
        throw new RuntimeException("Limit requires an integer parameter");
      }
      if (variableLimit <= 0)
        throw new RuntimeException(
            "Limit requires a positive integer parameter");
      this.setLimit(variableLimit);
    }
    Result inp = null;
    while (true) {
      inp = processInput();
      if (inp.returnStatus == POStatus.STATUS_EOP
          || inp.returnStatus == POStatus.STATUS_ERR)
        break;

      illustratorMarkup(inp.result, null, 0);
      // illustrator ignore LIMIT before the post processing
      if ((illustrator == null || illustrator.getOriginalLimit() != -1)
          && soFar >= mLimit)
        inp.returnStatus = POStatus.STATUS_EOP;

      soFar++;
      break;
    }

    return inp;
  }

  @Override
  public String name() {
    return getAliasString() + "Limit - " + mKey.toString();
  }

  @Override
  public boolean supportsMultipleInputs() {
    return false;
  }

  @Override
  public boolean supportsMultipleOutputs() {
    return true;
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitLimit(this);
  }

  @Override
  public void reset() {
    soFar = 0;
  }

  @Override
  public POLimit clone() throws CloneNotSupportedException {
    POLimit newLimit = new POLimit(new OperatorKey(this.mKey.scope,
        NodeIdGenerator.getGenerator().getNextNodeId(this.mKey.scope)),
        this.requestedParallelism, this.inputs);
    newLimit.mLimit = this.mLimit;
    newLimit.expressionPlan = this.expressionPlan.clone();
    newLimit.addOriginalLocation(alias, getOriginalLocations());
    return newLimit;
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    if (illustrator != null) {
      ExampleTuple tIn = (ExampleTuple) in;
      illustrator.getEquivalenceClasses().get(eqClassIndex).add(tIn);
      illustrator.addData((Tuple) in);
    }
    return (Tuple) in;
  }
}
