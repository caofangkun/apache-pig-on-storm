package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The POPartition is a special operator.
 * 
 */
public class POPartition extends PhysicalOperator {

  private static final long serialVersionUID = 1L;

  protected String sourceAlias = null;

  private static final List<PhysicalPlan> EMPTY_PLAN_LIST = new ArrayList<PhysicalPlan>();

  protected List<PhysicalPlan> plans;

  protected List<ExpressionOperator> leafOps;

  protected byte keyType;
  //
  protected boolean isNewPartition = true;

  private Map<String, List<PhysicalOperator>> partitionSeen;

  public POPartition(OperatorKey k) {
    this(k, -1, null);
  }

  public POPartition(OperatorKey k, int rp) {
    this(k, rp, null);
  }

  public POPartition(OperatorKey k, List<PhysicalOperator> inputs) {
    this(k, -1, inputs);
  }

  public POPartition(OperatorKey k, int rp, List<PhysicalOperator> inputs) {
    super(k, rp, inputs);
    //
    leafOps = new ArrayList<ExpressionOperator>();
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

  public boolean isNewPartition() {
    return isNewPartition;
  }

  public void setNewPartition(boolean isNewPartition) {
    this.isNewPartition = isNewPartition;
  }

  public String getSourceAlias() {
    if (getInputs().size() > 0) {
      return getInputs().get(0).getAlias();
    }
    return sourceAlias;
  }

  public PhysicalOperator getSourceOperator() {
    return getInputs().get(0);
  }

  @Override
  public String name() {
    return getAliasString() + "Partition" + "["
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
    v.visitPartition(this);
  }

  @Override
  public Result processDataBags(DataBag... bags) throws ExecException {
    DataBag bag = bags[0];
    Result r = new Result();
    r.result = bag;
    r.returnStatus = POStatus.STATUS_OK;
    // TODO only for test
    return r;
  }

  public byte getKeyType() {
    return keyType;
  }

  public void setKeyType(byte keyType) {
    this.keyType = keyType;
  }

  public List<PhysicalPlan> getPlans() {
    return (plans == null) ? EMPTY_PLAN_LIST : plans;
  }

  public void setPlans(List<PhysicalPlan> plans) throws PlanException {
    this.plans = plans;
    leafOps.clear();
    for (PhysicalPlan plan : plans) {
      ExpressionOperator leaf = (ExpressionOperator) plan.getLeaves().get(0);
      leafOps.add(leaf);
    }
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    return null;
  }

  public PhysicalPlan getPlan() {
    // TODO
    return (plans == null) ? null : plans.get(0);
  }

}
