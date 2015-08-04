package org.apache.storm.executionengine.topologyLayer.plans;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POBind;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

public class PartitionDecomposer extends PhyPlanVisitor {

  public PartitionDecomposer(PhysicalPlan plan) {
    // super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
    super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));

  }

  @Override
  public void visitPartition(POPartition pt) throws VisitorException {

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
        deComposer(bind, partition);
      }
    }
  }

  private void deComposer(POBind bind, POPartition partition) {
    try {
      mPlan.remove(bind);
      if (bind.containsConnection(partition.getAlias())) {
        List<PhysicalOperator> tos = bind.getConnection(partition.getAlias());
        for (PhysicalOperator to : tos) {
          mPlan.connect(partition, to);
        }
      }
    } catch (PlanException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
