package org.apache.storm.executionengine.topologyLayer.plans;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POBind;
import org.apache.storm.executionengine.topologyLayer.TopologyCompilerException;
import org.apache.storm.executionengine.topologyLayer.TopologyOperator;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

import com.google.common.collect.Sets;

public class PartitionTopologyChecker extends TopologyOperatorPlanVisitor {

  private Set<String> allInputAlias = new HashSet<String>();

  public PartitionTopologyChecker(TopologyOperatorPlan plan) {

    // super(plan, new DepthFirstWalker<TopologyOperator,
    // TopologyOperatorPlan>(plan));

    super(plan,
        new DependencyOrderWalker<TopologyOperator, TopologyOperatorPlan>(plan));
  }

  public void reset() {
    allInputAlias.clear();
  }

  private Set<String> collectAlias(TopologyOperator topo) {
    Set<String> allInputAlias = new HashSet<String>();
    Iterator<PhysicalOperator> poIter = topo.topoPlan.iterator();
    while (poIter.hasNext()) {
      PhysicalOperator po = poIter.next();
      List<PhysicalOperator> inputs = po.getInputs();
      if (inputs != null) {
        for (PhysicalOperator input : inputs) {
          if (input instanceof POBind) {
            allInputAlias.addAll(((POBind) input).getPartitionSeen().keySet());
          } else {
            allInputAlias.add(input.getAlias());
          }
        }
      }
    }
    return allInputAlias;
  }

  /**
   * TopologyCompilerException
   */
  @Override
  public void visitTopologyOperator(TopologyOperator topo)
      throws VisitorException {
    Set<String> localAlias = collectAlias(topo);
    if (!allInputAlias.isEmpty()) {
      Set<String> diff = Sets.intersection(allInputAlias, localAlias);
      if (!diff.isEmpty()) {
        int errCode = 2034;
        String msg = "Error:The Relation " + diff.iterator().next()
            + " has not been PARTITION.";
        throw new TopologyCompilerException(msg, errCode);
      }
    }
    allInputAlias.addAll(localAlias);
  }

}
