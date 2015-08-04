package org.apache.storm.executionengine.topologyLayer.plans;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POBind;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

public class PartitionComposer extends PhyPlanVisitor {

  public PartitionComposer(PhysicalPlan plan) {
    // super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
    super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));

  }

  @Override
  public void visitPartition(POPartition pt) throws VisitorException {
    List<PhysicalOperator> successors = new ArrayList<PhysicalOperator>();
    List<PhysicalOperator> succes = mPlan.getSuccessors(pt);
    if (succes != null) {
      for (PhysicalOperator po : succes) {
        if (!(po instanceof POBind)) {
          successors.add(po);
        }
      }
      pt.putConnection(pt.getAlias(), successors);
    }
  }

  @Override
  public void visitBind(POBind bind) throws VisitorException {
    List<PhysicalOperator> inputs = bind.getInputs();
    List<PhysicalOperator> ops = new ArrayList<PhysicalOperator>(inputs.size());
    ops.addAll(inputs);
    Collections.sort(ops);
    for (PhysicalOperator po : ops) {
      if (po instanceof POPartition) {
        POPartition partition = (POPartition) po;
        partition.setNewPartition(false);
        //
        composer(bind, partition);
      }
    }
  }

  private void composer(POBind bind, POPartition partition) {
    List<PhysicalOperator> successors = new ArrayList<PhysicalOperator>();
    successors.addAll(mPlan.getSuccessors(partition));
    //
    successors.remove(bind);
    bind.putConnection(partition.getAlias(), successors);
    //
    Iterator<PhysicalOperator> it = successors.iterator();
    try {
      while (it.hasNext()) {
        PhysicalOperator to = it.next();
        if (!(to instanceof POBind)) {
          mPlan.disconnect(partition, to);
          // mPlan.connect(bind, to);
          if (!mPlan.pathExists(bind, to)) {
            mPlan.connect(bind, to);
          }
        }
      }
    } catch (PlanException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
