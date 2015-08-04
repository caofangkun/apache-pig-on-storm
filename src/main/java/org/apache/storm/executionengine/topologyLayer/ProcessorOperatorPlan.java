package org.apache.storm.executionengine.topologyLayer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;

public class ProcessorOperatorPlan extends OperatorPlan<ProcessorOperator> {

  private static final long serialVersionUID = 1L;

  public ProcessorOperatorPlan() {
  }

  public void explain(PrintStream ps, String format, boolean verbose)
      throws VisitorException {
    if (format.equals("xml")) {
      ps.println("<ProcessorOperatorPlan>XML Not Supported</ProcessorOperatorPlan>");
      return;
    }
    ps.println("#--------------------------------------------------");
    ps.println("# Processor Operator Plan:                          ");
    ps.println("#--------------------------------------------------");

    if (format.equals("dot")) {
      ProcessPlanDotPrinter printer = new ProcessPlanDotPrinter(this, ps);
      printer.setVerbose(verbose);
      printer.dump();
    } else {
      ProcessPlanPrinter printer = new ProcessPlanPrinter(ps, this);
      printer.setVerbose(verbose);
      printer.visit();

    }
    ps.println("");
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
