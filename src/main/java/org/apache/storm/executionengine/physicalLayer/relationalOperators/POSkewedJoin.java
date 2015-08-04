package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

/**
 * The PhysicalOperator that represents a skewed join. It must have two inputs.
 * This operator does not do any actually work, it is only a place holder. When
 * it is translated into MR plan, a POSkewedJoin is translated into a sampling
 * job and a join job.
 * 
 * 
 */
public class POSkewedJoin extends PhysicalOperator {

  private static final long serialVersionUID = 1L;
  private boolean[] mInnerFlags;

  // The schema is used only by the MRCompiler to support outer join
  transient private List<Schema> inputSchema = new ArrayList<Schema>();

  // physical plans to retrive join keys
  // the key of this <code>MultiMap</code> is the PhysicalOperator that
  // corresponds to an input
  // the value is a list of <code>PhysicalPlan</code> to retrieve each join
  // key for this input
  private MultiMap<PhysicalOperator, PhysicalPlan> mJoinPlans;

  public POSkewedJoin(OperatorKey k) {
    this(k, -1, null, null);
  }

  public POSkewedJoin(OperatorKey k, int rp) {
    this(k, rp, null, null);
  }

  public POSkewedJoin(OperatorKey k, List<PhysicalOperator> inp, boolean[] flags) {
    this(k, -1, inp, flags);
  }

  public POSkewedJoin(OperatorKey k, int rp, List<PhysicalOperator> inp,
      boolean[] flags) {
    super(k, rp, inp);
    if (flags != null) {
      // copy the inner flags
      mInnerFlags = new boolean[flags.length];
      for (int i = 0; i < flags.length; i++) {
        mInnerFlags[i] = flags[i];
      }
    }
  }

  public boolean[] getInnerFlags() {
    return mInnerFlags;
  }

  public MultiMap<PhysicalOperator, PhysicalPlan> getJoinPlans() {
    return mJoinPlans;
  }

  public void setJoinPlans(MultiMap<PhysicalOperator, PhysicalPlan> joinPlans) {
    mJoinPlans = joinPlans;
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitSkewedJoin(this);
  }

  @Override
  public String name() {
    return getAliasString() + "SkewedJoin[" + DataType.findTypeName(resultType)
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

  public void addSchema(Schema s) {
    inputSchema.add(s);
  }

  public Schema getSchema(int i) {
    return inputSchema.get(i);
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    return null;
  }
}
