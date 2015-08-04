package org.apache.storm.executionengine.topologyLayer.plans;

import org.apache.storm.executionengine.topologyLayer.TopologyOperator;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor for the TopologyOperatorPlan class
 * 
 */
public class TopologyOperatorPlanVisitor extends
    PlanVisitor<TopologyOperator, TopologyOperatorPlan> {

  public TopologyOperatorPlanVisitor(TopologyOperatorPlan plan,
      PlanWalker<TopologyOperator, TopologyOperatorPlan> walker) {
    super(plan, walker);
  }

  public void visitTopologyOperator(TopologyOperator topo)
      throws VisitorException {
  }

}
