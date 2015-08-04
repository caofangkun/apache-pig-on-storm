package org.apache.storm.executionengine.topologyLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

public class ScalarPhyFinder extends PhyPlanVisitor {

  List<PhysicalOperator> scalars = new ArrayList<PhysicalOperator>();

  public ScalarPhyFinder(PhysicalPlan plan) {
    super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
  }

  public List<PhysicalOperator> getScalars() {
    return scalars;
  }

  @Override
  public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
    if (userFunc.getReferencedOperator() != null) {
      scalars.add(userFunc.getReferencedOperator());
    }
  }
}
