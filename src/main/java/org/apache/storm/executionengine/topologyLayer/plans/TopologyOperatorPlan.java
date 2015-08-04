package org.apache.storm.executionengine.topologyLayer.plans;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.topologyLayer.TopologyOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A Plan used to create the plan of Topology Operators which can be converted
 * into the Topology.
 */
public class TopologyOperatorPlan extends OperatorPlan<TopologyOperator> {

  private static final long serialVersionUID = 1L;

  public TopologyOperatorPlan() {

  }

  public void explain(PrintStream ps, String format, boolean verbose)
      throws VisitorException {
    if (format.equals("xml")) {
      ps.println("<TopologyOperatorPlan>XML Not Supported</TopologyOperatorPlan>");
      return;
    }
    ps.println("#--------------------------------------------------");
    ps.println("# Topology Operator Plan:                          ");
    ps.println("#--------------------------------------------------");

    if (format.equals("dot")) {
      DotTOPrinter printer = new DotTOPrinter(this, ps);
      printer.setVerbose(verbose);
      printer.dump();
    } else {
      TopologyPrinter printer = new TopologyPrinter(ps, this);
      printer.setVerbose(verbose);
      printer.visit();

    }
    ps.println("");
  }

  public TopologyOperator getTOByPOAlias(String alias) {
    Iterator<TopologyOperator> it = this.iterator();
    while (it.hasNext()) {
      TopologyOperator to = it.next();
      PhysicalPlan plan = to.topoPlan;
      if (plan != null && plan.hasOperator(alias)) {
        return to;
      }
    }

    return null;
  }

  @Override
  public String toString() {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    try {
      explain(ps, "", true);
    } catch (FrontendException e) {
      return "";
    }
    return os.toString();
  }

}
