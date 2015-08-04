package org.apache.storm.executionengine.topologyLayer;

import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class ProcessorPlanVistor extends
    PlanVisitor<ProcessorOperator, ProcessorOperatorPlan> {

  protected ProcessorPlanVistor(ProcessorOperatorPlan plan,
      PlanWalker<ProcessorOperator, ProcessorOperatorPlan> walker) {
    super(plan, walker);
    // TODO Auto-generated constructor stub
  }

  public void visitProcessorOperator(ProcessorOperator topo)
      throws VisitorException {

  }
}
