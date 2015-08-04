package org.apache.storm.executionengine.topologyLayer.plans;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.storm.executionengine.topologyLayer.TopologyOperator;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor mechanism printing out the Topology plan.
 */
public class TopologyPrinter extends TopologyOperatorPlanVisitor {

  private PrintStream mStream = null;

  private boolean isVerbose = true;

  private String USep = "|   |\n|   ";

  private String TAB1 = "    ";

  private String TABMore = "|   ";

  /**
   * @param ps
   *          PrintStream to output plan information to
   * @param plan
   *          Topology plan to print
   */
  public TopologyPrinter(PrintStream ps, TopologyOperatorPlan plan) {
    super(plan, new DepthFirstWalker<TopologyOperator, TopologyOperatorPlan>(
        plan));
    mStream = ps;
  }

  public void setVerbose(boolean verbose) {
    isVerbose = verbose;
  }

  @Override
  public void visitTopologyOperator(TopologyOperator topo)
      throws VisitorException {
    mStream.println("Topology Operator " + topo.getOperatorKey().toString());
    //
    if (topo.getInputPartitions().size() > 0) {
      mStream.println("InputPartitions: ");
      for (POPartition partition : topo.getInputPartitions()) {
        StringBuilder sb = new StringBuilder(partition.name() + "\n");
        sb.append(planString(partition.getPlans()));
        mStream.println(sb.toString());
      }

    }
    if (topo.getOutputPartitions().size() > 0) {
      mStream.println("OutputPartitions: ");
      for (POPartition partition : topo.getOutputPartitions()) {
        StringBuilder sb = new StringBuilder(partition.name() + "\n");
        sb.append(planString(partition.getPlans()));
        mStream.println(sb.toString());
      }

    }
    //
    if (topo.topoPlan != null && topo.topoPlan.size() > 0) {
      mStream.println("Sub Physical Plan");
      PlanPrinter<PhysicalOperator, PhysicalPlan> printer = new PlanPrinter<PhysicalOperator, PhysicalPlan>(
          topo.topoPlan, mStream);
      printer.setVerbose(isVerbose);
      printer.visit();
      mStream.println("--------");
    }
    mStream.println("");
  }

  private String planString(PhysicalPlan pp) {
    StringBuilder sb = new StringBuilder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    if (pp != null)
      pp.explain(baos, isVerbose);
    else
      return "";
    sb.append(USep);
    sb.append(shiftStringByTabs(baos.toString(), 2));
    return sb.toString();
  }

  private String planString(List<PhysicalPlan> lep) {
    StringBuilder sb = new StringBuilder();
    if (lep != null)
      for (PhysicalPlan ep : lep) {
        sb.append(planString(ep));
      }
    return sb.toString();
  }

  private String shiftStringByTabs(String DFStr, int TabType) {
    StringBuilder sb = new StringBuilder();
    String[] spl = DFStr.split("\n");

    String tab = (TabType == 1) ? TAB1 : TABMore;

    sb.append(spl[0] + "\n");
    for (int i = 1; i < spl.length; i++) {
      sb.append(tab);
      sb.append(spl[i]);
      sb.append("\n");
    }
    return sb.toString();
  }

}
