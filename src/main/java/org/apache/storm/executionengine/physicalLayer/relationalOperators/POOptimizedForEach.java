package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A specialized version of POForeach with the difference that in getNext(), it
 * knows that "input" has already been attached by its input operator which
 * SHOULD be POJoinPackage
 */

// We intentionally skip type checking in backend for performance reasons
@SuppressWarnings("unchecked")
public class POOptimizedForEach extends POForEach {

  /**
     * 
     */
  private static final long serialVersionUID = 1L;

  public POOptimizedForEach(OperatorKey k) {
    this(k, -1, null, null);
  }

  public POOptimizedForEach(OperatorKey k, int rp, List inp) {
    this(k, rp, inp, null);
  }

  public POOptimizedForEach(OperatorKey k, int rp) {
    this(k, rp, null, null);
  }

  public POOptimizedForEach(OperatorKey k, List inp) {
    this(k, -1, inp, null);
  }

  public POOptimizedForEach(OperatorKey k, int rp, List<PhysicalPlan> inp,
      List<Boolean> isToBeFlattened) {
    super(k, rp);
    setUpFlattens(isToBeFlattened);
    this.inputPlans = inp;
    getLeaves();
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitPOOptimizedForEach(this);
  }

  @Override
  public String name() {
    String fString = getFlatStr();
    return "Optimized For Each" + "(" + fString + ")" + "["
        + DataType.findTypeName(resultType) + "]" + " - " + mKey.toString();
  }

  /**
   * Calls getNext on the generate operator inside the nested physical plan and
   * returns it maintaining an additional state to denote the begin and end of
   * the nested plan processing.
   */
  @Override
  public Result getNextTuple() throws ExecException {
    Result res = null;
    Result inp = null;
    // The nested plan is under processing
    // So return tuples that the generate oper
    // returns
    if (processingPlan) {
      while (true) {
        res = processPlan();
        if (res.returnStatus == POStatus.STATUS_OK) {
          return res;
        }
        if (res.returnStatus == POStatus.STATUS_EOP) {
          processingPlan = false;
          for (PhysicalPlan plan : inputPlans)
            plan.detachInput();
          return res;
        }
        if (res.returnStatus == POStatus.STATUS_ERR) {
          return res;
        }
        if (res.returnStatus == POStatus.STATUS_NULL) {
          continue;
        }
      }
    }
    // The nested plan processing is done or is
    // yet to begin. So process the input and start
    // nested plan processing on the input tuple
    // read
    while (true) {

      // we know that input has been attached
      attachInputToPlans(input);
      detachInput();
      res = processPlan();

      processingPlan = true;

      return res;
    }
  }

  /**
   * Make a deep copy of this operator.
   * 
   * @throws CloneNotSupportedException
   */
  @Override
  public POOptimizedForEach clone() throws CloneNotSupportedException {
    List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>(inputPlans.size());
    for (PhysicalPlan plan : inputPlans) {
      plans.add(plan.clone());
    }
    List<Boolean> flattens = null;
    if (isToBeFlattenedArray != null) {
      flattens = new ArrayList<Boolean>(isToBeFlattenedArray.length);
      for (boolean b : isToBeFlattenedArray) {
        flattens.add(b);
      }
    }
    return new POOptimizedForEach(new OperatorKey(mKey.scope, NodeIdGenerator
        .getGenerator().getNextNodeId(mKey.scope)), requestedParallelism,
        plans, flattens);
  }
}
