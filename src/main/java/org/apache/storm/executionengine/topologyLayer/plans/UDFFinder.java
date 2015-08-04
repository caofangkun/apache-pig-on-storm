package org.apache.storm.executionengine.topologyLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class UDFFinder extends PhyPlanVisitor {

  List<String> UDFs;

  DepthFirstWalker<PhysicalOperator, PhysicalPlan> dfw;

  public UDFFinder() {
    this(null, null);
  }

  public UDFFinder(PhysicalPlan plan,
      PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
    super(plan, walker);
    UDFs = new ArrayList<String>();
    dfw = new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(null);
  }

  public List<String> getUDFs() {
    return UDFs;
  }

  public void setPlan(PhysicalPlan plan) {
    mPlan = plan;
    dfw.setPlan(plan);
    mCurrentWalker = dfw;
    UDFs.clear();
  }

  @Override
  public void visitSort(POSort op) throws VisitorException {
    if (op.getMSortFunc() != null)
      UDFs.add(op.getMSortFunc().getFuncSpec().toString());
  }

  @Override
  public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
    UDFs.add(userFunc.getFuncSpec().toString());
  }

  @Override
  public void visitComparisonFunc(POUserComparisonFunc compFunc)
      throws VisitorException {
    UDFs.add(compFunc.getFuncSpec().toString());
  }

  @Override
  public void visitCast(POCast op) {
    if (op.getFuncSpec() != null)
      UDFs.add(op.getFuncSpec().toString());
  }

}
