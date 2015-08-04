package org.apache.storm.executionengine.topologyLayer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

public class ProcessPlanPrinter extends ProcessorPlanVistor {

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
  public ProcessPlanPrinter(PrintStream ps, ProcessorOperatorPlan plan) {
    super(plan, new DepthFirstWalker<ProcessorOperator, ProcessorOperatorPlan>(
        plan));
    mStream = ps;
  }

  public void setVerbose(boolean verbose) {
    isVerbose = verbose;
  }

  @Override
  public void visitProcessorOperator(ProcessorOperator topo)
      throws VisitorException {
    mStream.println("Processor Operator " + topo.getOperatorKey().toString());
    //
    if (topo.getInputMap().size() > 0) {
      mStream.println("Input: ");
      for (Map.Entry<String, Set<String>> entry : topo.getInputMap().entrySet()) {
        StringBuilder sb = new StringBuilder(entry.getKey() + "\n");
        sb.append(entry.getValue());
        mStream.println(sb.toString());
      }

    }
    if (topo.getOutputMap().size() > 0) {
      mStream.println("Output: ");
      for (Map.Entry<String, Set<String>> entry : topo.getOutputMap()
          .entrySet()) {
        StringBuilder sb = new StringBuilder(entry.getKey() + "\n");
        sb.append(entry.getValue());
        mStream.println(sb.toString());
      }

    }
    //
    if (topo.getInnerPlan() != null && topo.getInnerPlan().size() > 0) {
      mStream.println("Sub Physical Plan");
      PlanPrinter<PhysicalOperator, PhysicalPlan> printer = new PlanPrinter<PhysicalOperator, PhysicalPlan>(
          topo.getInnerPlan(), mStream);
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
