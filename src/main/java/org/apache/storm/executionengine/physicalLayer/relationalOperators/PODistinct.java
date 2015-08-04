package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalDistinctBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Find the distinct set of tuples in a bag. This is a blocking operator. All
 * the input is put in the hashset implemented in DistinctDataBag which also
 * provides the other DataBag interfaces.
 * 
 * 
 */
public class PODistinct extends PhysicalOperator implements Cloneable {

  private static final Log log = LogFactory.getLog(PODistinct.class);

  private static final long serialVersionUID = 1L;

  private boolean inputsAccumulated = false;

  private DataBag distinctBag = null;

  transient Iterator<Tuple> it;

  // PIG-3385: Since GlobalRearrange is not used by PODistinct, passing the
  // custom partioner through here
  protected String customPartitioner;

  public String getCustomPartitioner() {
    return customPartitioner;
  }

  public void setCustomPartitioner(String customPartitioner) {
    this.customPartitioner = customPartitioner;
  }

  public PODistinct(OperatorKey k, int rp, List<PhysicalOperator> inp) {
    super(k, rp, inp);
  }

  public PODistinct(OperatorKey k, int rp) {
    super(k, rp);
  }

  public PODistinct(OperatorKey k, List<PhysicalOperator> inp) {
    super(k, inp);
  }

  public PODistinct(OperatorKey k) {
    super(k);
  }

  @Override
  public boolean isBlocking() {
    return true;
  }

  @Override
  public Result processDataBags(DataBag... bags) throws ExecException {
    DataBag bag = bags[0];
    Result r = new Result();
    if (bag == null) {
      r.returnStatus = POStatus.STATUS_NULL;
      return r;
    }
    // distinctBag = BagFactory.getInstance().newDistinctBag();//default
    distinctBag = new InternalDistinctBag(3);

    distinctBag.addAll(bag);

    r.result = distinctBag;
    if (r.result == null) {
      r.returnStatus = POStatus.STATUS_NULL;
      reset();
    } else {
      r.returnStatus = POStatus.STATUS_OK;
    }
    return r;
  }

  @Override
  public Result getNextTuple() throws ExecException {
    if (!inputsAccumulated) {
      Result in = processInput();
      // distinctBag = BagFactory.getInstance().newDistinctBag();//default
      distinctBag = new InternalDistinctBag(3);
      while (in.returnStatus != POStatus.STATUS_EOP) {
        if (in.returnStatus == POStatus.STATUS_ERR) {
          log.error("Error in reading from inputs");
          return in;
          // continue;
        } else if (in.returnStatus == POStatus.STATUS_NULL) {
          // Ignore the null, read the next tuple. It's not clear
          // to me that we should ever get this, or if we should,
          // how it differs from EOP. But ignoring it here seems
          // to work.
          in = processInput();
          continue;
        }
        distinctBag.add((Tuple) in.result);
        illustratorMarkup(in.result, in.result, 0);
        in = processInput();
      }
      inputsAccumulated = true;
    }
    //
    if (it == null) {
      it = distinctBag.iterator();
    }
    res.result = it.next();
    if (res.result == null) {
      res.returnStatus = POStatus.STATUS_EOP;
      reset();
    } else {
      res.returnStatus = POStatus.STATUS_OK;
    }
    return res;
  }

  @Override
  public String name() {
    return getAliasString() + "PODistinct" + "["
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
  public void reset() {
    inputsAccumulated = false;
    distinctBag = null;
    it = null;
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitDistinct(this);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator
   * #clone()
   */
  @Override
  public PODistinct clone() throws CloneNotSupportedException {
    return new PODistinct(new OperatorKey(this.mKey.scope, NodeIdGenerator
        .getGenerator().getNextNodeId(this.mKey.scope)),
        this.requestedParallelism, this.inputs);
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    if (illustrator != null) {
      illustrator.getEquivalenceClasses().get(eqClassIndex).add((Tuple) out);
      illustrator.addData((Tuple) out);
    }
    return null;
  }
}
