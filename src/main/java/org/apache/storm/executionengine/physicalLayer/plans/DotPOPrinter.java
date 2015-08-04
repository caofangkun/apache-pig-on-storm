package org.apache.storm.executionengine.physicalLayer.plans;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POTap;
import org.apache.pig.impl.plan.DotPlanDumper;
import org.apache.pig.impl.plan.Operator;

/**
 * This class can print a physical plan in the DOT format. It uses clusters to
 * illustrate nesting. If "verbose" is off, it will skip any nesting.
 */
public class DotPOPrinter
    extends
    DotPlanDumper<PhysicalOperator, PhysicalPlan, PhysicalOperator, PhysicalPlan> {

  public DotPOPrinter(PhysicalPlan plan, PrintStream ps) {
    this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
        new HashSet<Operator>());
  }

  public DotPOPrinter(PhysicalPlan plan, PrintStream ps, boolean isSubGraph,
      Set<Operator> subgraphs, Set<Operator> multiInputSubgraphs,
      Set<Operator> multiOutputSubgraphs) {
    super(plan, ps, isSubGraph, subgraphs, multiInputSubgraphs,
        multiOutputSubgraphs);
  }

  @Override
  protected DotPlanDumper makeDumper(PhysicalPlan plan, PrintStream ps) {
    DotPOPrinter dumper = new DotPOPrinter(plan, ps, true, mSubgraphs,
        mMultiInputSubgraphs, mMultiOutputSubgraphs);
    dumper.setVerbose(this.isVerbose());
    return dumper;
  }

  @Override
  protected String getName(PhysicalOperator op) {
    return (op.name().split(" - "))[0];
  }

  @Override
  protected String[] getAttributes(PhysicalOperator op) {
    if (op instanceof POStore || op instanceof POLoad || op instanceof POTap) {
      String[] attributes = new String[3];
      String name = getName(op);
      int idx = name.lastIndexOf(":");
      if (idx != -1) {
        String part1 = name.substring(0, idx);
        String part2 = name.substring(idx + 1, name.length());
        name = part1 + ",\\n" + part2;
      }
      attributes[0] = "label=\"" + name + "\"";
      attributes[1] = "style=\"filled\"";
      attributes[2] = "fillcolor=\"gray\"";
      return attributes;
    } else {
      return super.getAttributes(op);
    }
  }

  @Override
  protected Collection<PhysicalPlan> getMultiOutputNestedPlans(
      PhysicalOperator op) {
    Collection<PhysicalPlan> plans = new LinkedList<PhysicalPlan>();

    if (op instanceof POSplit) {
      plans.addAll(((POSplit) op).getPlans());
    } else if (op instanceof PODemux) {
      Set<PhysicalPlan> pl = new HashSet<PhysicalPlan>();
      pl.addAll(((PODemux) op).getPlans());
      plans.addAll(pl);
    }

    return plans;
  }

  @Override
  protected Collection<PhysicalPlan> getNestedPlans(PhysicalOperator op) {
    Collection<PhysicalPlan> plans = new LinkedList<PhysicalPlan>();

    if (op instanceof POPartition) {
      // plans.add(((POPartition) op).getPlan());
      plans.addAll(((POPartition) op).getPlans());
    } else if (op instanceof POFilter) {
      plans.add(((POFilter) op).getPlan());
    } else if (op instanceof POForEach) {
      plans.addAll(((POForEach) op).getInputPlans());
    } else if (op instanceof POSort) {
      plans.addAll(((POSort) op).getSortPlans());
    } else if (op instanceof PORank) {
      plans.addAll(((PORank) op).getRankPlans());
    } else if (op instanceof POCounter) {
      plans.addAll(((POCounter) op).getCounterPlans());
    } else if (op instanceof POLocalRearrange) {
      plans.addAll(((POLocalRearrange) op).getPlans());
    } else if (op instanceof POFRJoin) {
      POFRJoin frj = (POFRJoin) op;
      List<List<PhysicalPlan>> joinPlans = frj.getJoinPlans();
      if (joinPlans != null) {
        for (List<PhysicalPlan> list : joinPlans) {
          plans.addAll(list);
        }
      }
    } else if (op instanceof POSkewedJoin) {
      POSkewedJoin skewed = (POSkewedJoin) op;
      Collection<PhysicalPlan> joinPlans = skewed.getJoinPlans().values();
      if (joinPlans != null) {
        plans.addAll(joinPlans);
      }
    }

    return plans;
  }

}
