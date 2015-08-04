package org.apache.storm.executionengine.topologyLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

public class WindowFinder extends PhyPlanVisitor {

  public WindowFinder(PhysicalPlan plan) {
    super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));

  }

  @Override
  public void visitPOForEach(POForEach foreach) throws VisitorException {
    if (foreach.isWindowMode()) {
      List<PhysicalOperator> successors = new ArrayList<PhysicalOperator>();
      List<PhysicalOperator> succes = mPlan.getSuccessors(foreach);
      if (succes != null) {
        for (PhysicalOperator po : succes) {
          successors.add(po);
        }
        
        //foreach.putConnection(foreach.getAlias(), successors);
        
      }
    }
  }

  @Override
  public void visitStream(POStream stream) throws VisitorException {
    if (stream.isWindowOperator()) {
      List<PhysicalOperator> successors = new ArrayList<PhysicalOperator>();
      List<PhysicalOperator> succes = mPlan.getSuccessors(stream);
      if (succes != null) {
        for (PhysicalOperator po : succes) {
          successors.add(po);
        }
        
        //stream.putConnection(stream.getAlias(), successors);
        
      }
    }
  }

}
