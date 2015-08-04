package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The bind operator that combines the two inputs into a single TO.
 * 
 */
public class POBind extends PhysicalOperator {

  private static final long serialVersionUID = 1L;

  boolean nextReturnEOP = false;

  private static Result eopResult = new Result(POStatus.STATUS_EOP, null);

  private Map<String, List<PhysicalOperator>> partitionSeen;

  public POBind(OperatorKey k) {
    this(k, -1, null);
  }

  public POBind(OperatorKey k, int rp) {
    this(k, rp, null);
  }

  public POBind(OperatorKey k, List<PhysicalOperator> inp) {
    this(k, -1, inp);
  }

  public POBind(OperatorKey k, int rp, List<PhysicalOperator> inp) {
    super(k, rp, inp);
    partitionSeen = new HashMap<String, List<PhysicalOperator>>();
  }

  public boolean containsConnection(String from) {
    return partitionSeen.containsKey(from);
  }

  public void putConnection(String from, List<PhysicalOperator> to) {
    partitionSeen.put(from, to);
  }

  public List<PhysicalOperator> getConnection(String from) {
    return partitionSeen.get(from);
  }

  public Map<String, List<PhysicalOperator>> getPartitionSeen() {
    return partitionSeen;
  }

  @Override
  public void setInputs(List<PhysicalOperator> inputs) {
    super.setInputs(inputs);
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitBind(this);
  }

  @Override
  public String name() {
    return getAliasString() + "Bind" + "[" + DataType.findTypeName(resultType)
        + "]" + " - " + mKey.toString();
  }

  @Override
  public boolean supportsMultipleInputs() {
    return true;
  }

  @Override
  public boolean supportsMultipleOutputs() {
    return true;
  }

  @Override
  public Result processDataBags(DataBag... bags) throws ExecException {
    Result r = new Result();
    r.result = bags;
    r.returnStatus = POStatus.STATUS_OK;
    // TODO only for test
    return r;
  }

  @Override
  public Result getNextTuple() throws ExecException {
    if (nextReturnEOP) {
      nextReturnEOP = false;
      return eopResult;
    } else {
      res.result = input;
      res.returnStatus = POStatus.STATUS_OK;
      detachInput();
      nextReturnEOP = true;
      illustratorMarkup(res.result, res.result, 0);
      return res;
    }

  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    return null;
  }
}
