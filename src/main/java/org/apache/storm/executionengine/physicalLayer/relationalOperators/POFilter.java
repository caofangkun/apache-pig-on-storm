package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This is an implementation of the Filter operator. It has an Expression Plan
 * that decides whether the input tuple should be filtered or passed through. To
 * avoid many function calls, the filter operator, stores the Comparison
 * Operator that is the root of the Expression Plan and uses its getNext
 * directly.
 * 
 * Since the filter is supposed to return tuples only, getNext is not supported
 * on any other data type.
 * 
 */
public class POFilter extends PhysicalOperator {

  private static final Log log = LogFactory.getLog(POFilter.class);
  /**
     * 
     */
  private static final long serialVersionUID = 1L;

  // The expression plan
  PhysicalPlan plan;

  // The root comparison operator of the expression plan
  // ComparisonOperator comOp;
  PhysicalOperator comOp;

  // The operand type for the comparison operator needed
  // to call the comparison operators getNext with the
  // appropriate type
  byte compOperandType;

  public POFilter(OperatorKey k) {
    this(k, -1, null);
  }

  public POFilter(OperatorKey k, int rp) {
    this(k, rp, null);
  }

  public POFilter(OperatorKey k, List<PhysicalOperator> inputs) {
    this(k, -1, inputs);
  }

  public POFilter(OperatorKey k, int rp, List<PhysicalOperator> inputs) {
    super(k, rp, inputs);
  }

  /**
   * Attaches the proccesed input tuple to the expression plan and checks if
   * comparison operator returns a true. If so the tuple is not filtered and let
   * to pass through. Else, further input is processed till a tuple that can be
   * passed through is found or EOP is reached.
   */
  @Override
  public Result getNextTuple() throws ExecException {
    Result res = null;
    Result inp = null;
    while (true) {
      inp = processInput();
      if (inp.returnStatus == POStatus.STATUS_EOP
          || inp.returnStatus == POStatus.STATUS_ERR)
        break;
      if (inp.returnStatus == POStatus.STATUS_NULL) {
        continue;
      }

      plan.attachInput((Tuple) inp.result);
      res = comOp.getNextBoolean();//
      plan.detachInput();

      if (res.returnStatus != POStatus.STATUS_OK
          && res.returnStatus != POStatus.STATUS_NULL)
        return res;

      if (res.result != null) {
        illustratorMarkup(inp.result, inp.result, (Boolean) res.result ? 0 : 1);
        if ((Boolean) res.result)
          return inp;
      }
    }
    return inp;
  }

  public Result processDataBags(DataBag... bags) throws ExecException {
    DataBag bag = bags[0];
    Result r = new Result();
    if (bag == null || bag.size() == 0) {
      r.returnStatus = POStatus.STATUS_NULL;
      return r;
    }

    Iterator<Tuple> it = bag.iterator();
    List<Tuple> tuples = new ArrayList<Tuple>();
    //
    Result res = null;
    Tuple inp = null;
    while (it.hasNext()) {
      inp = it.next();
      plan.attachInput(inp);
      res = comOp.getNextBoolean();
      plan.detachInput();

      if (res.returnStatus != POStatus.STATUS_OK
          && res.returnStatus != POStatus.STATUS_NULL) {
        log.error("Invalid result while executing the boolean expression plan : "
            + plan.toString());
      }

      if (res.result != null) {
        if ((Boolean) res.result) {
          tuples.add(inp);
          log.debug("Filter output:" + alias + ":" + inp);
        }
      }
    }
    //
    if (tuples.size() == 0) {
      r.returnStatus = POStatus.STATUS_NULL;
      r.result = null;
    } else {
      r.returnStatus = POStatus.STATUS_OK;
      r.result = new NonSpillableDataBag(tuples);
    }
    return r;
  }

  @Override
  public String name() {
    return getAliasString() + "Filter" + "["
        + DataType.findTypeName(resultType) + "]" + " - " + mKey.toString();
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
    v.visitFilter(this);
  }

  public void setPlan(PhysicalPlan plan) {
    this.plan = plan;
    comOp = plan.getLeaves().get(0);
    // compOperandType = comOp.getOperandType();
  }

  public PhysicalPlan getPlan() {
    return plan;
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    if (illustrator != null) {
      int index = 0;
      for (int i = 0; i < illustrator.getSubExpResults().size(); ++i) {
        if (!illustrator.getSubExpResults().get(i)[0])
          index += (1 << i);
      }
      if (index < illustrator.getEquivalenceClasses().size())
        illustrator.getEquivalenceClasses().get(index).add((Tuple) in);
      if (eqClassIndex == 0) // add only qualified record
        illustrator.addData((Tuple) out);
    }
    return (Tuple) out;
  }
}
