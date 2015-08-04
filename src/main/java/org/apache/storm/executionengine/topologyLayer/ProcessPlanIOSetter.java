package org.apache.storm.executionengine.topologyLayer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

public class ProcessPlanIOSetter extends ProcessorPlanVistor {

  public ProcessPlanIOSetter(ProcessorOperatorPlan plan) {
    super(plan,
        new DependencyOrderWalker<ProcessorOperator, ProcessorOperatorPlan>(
            plan));
  }

  private Map<String, Set<String>> buildWindowInput(ProcessorOperator topo) {
    Map<String, Set<String>> fromToMap = new HashMap<String, Set<String>>();
    
//    for (Map.Entry<String, List<PhysicalOperator>> entry : topo
//        .getWindowInput().entrySet()) {
//      String key = entry.getKey();
//      List<PhysicalOperator> pos = entry.getValue();
//      Set<String> toSet = new HashSet<String>();
//      for (PhysicalOperator po : pos) {
//        if (topo.getInnerPlan().getOperatorKey(po) != null) {
//          toSet.add(po.getAlias());
//        }
//      }
//      fromToMap.put(key, toSet);
//    }
    
    return fromToMap;
  }

  private Map<String, Set<String>> buildWindowOutput(ProcessorOperator topo) {
    Map<String, Set<String>> fromToMap = new HashMap<String, Set<String>>();
    
//    for (Map.Entry<String, List<PhysicalOperator>> entry : topo
//        .getWindowOutput().entrySet()) {
//      String key = entry.getKey();
//      List<PhysicalOperator> pos = entry.getValue();
//      Set<String> toSet = new HashSet<String>();
//      for (PhysicalOperator po : pos) {
//        if (topo.getInnerPlan().getOperatorKey(po) == null) {
//          toSet.add(po.getAlias());
//        }
//      }
//      fromToMap.put(key, toSet);
//    }
    
    return fromToMap;
  }

  /**
   * TopologyCompilerException
   */
  @Override
  public void visitProcessorOperator(ProcessorOperator topo)
      throws VisitorException {
    topo.getInputMap().putAll(buildWindowInput(topo));
    topo.getOutputMap().putAll(buildWindowOutput(topo));
  }

}
