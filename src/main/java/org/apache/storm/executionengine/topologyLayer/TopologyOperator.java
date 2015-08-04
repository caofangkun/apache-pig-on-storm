package org.apache.storm.executionengine.topologyLayer;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POTap;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.storm.executionengine.topologyLayer.plans.TopologyOperatorPlanVisitor;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

/**
 * An operator model for TopologyOperator.
 */
public class TopologyOperator extends Operator<TopologyOperatorPlanVisitor> {

  private static final long serialVersionUID = 1L;

  // The sub physical plan that should be executed
  public PhysicalPlan topoPlan;

  // Indicates that the map plan creation
  // is complete
  boolean topoDone = false;

  // Indicates that there is an operator which uses endOfAllInput flag in the
  // plan
  boolean endOfAllInputInMap = false;

  // Indicates if this job is an order by job
  boolean globalSort = false;

  // Indicates if this is a limit after a sort
  boolean limitAfterSort = false;

  // Indicate if the entire purpose for this map reduce job is doing limit,
  // does not change
  // anything else. This is to help POPackageAnnotator to find the right
  // POPackage to annotate
  boolean limitOnly = false;

  // The sort order of the columns;
  // asc is true and desc is false
  boolean[] sortOrder;

  // Sort order for secondary keys;
  boolean[] secondarySortOrder;

  public Set<String> UDFs;

  public Set<PhysicalOperator> scalars;

  // Indicates if a UDF comparator is used
  boolean isUDFComparatorUsed = false;

  transient NodeIdGenerator nig;

  private String scope;

  int requestedParallelism = -1;

  // estimated at runtime
  int estimatedParallelism = -1;

  // calculated at runtime
  int runtimeParallelism = -1;

  /* Name of the Custom Partitioner used */
  String customPartitioner = null;

  // Map of the physical operator in physical plan to the one in MR plan: only
  // needed
  // if the physical operator is changed/replaced in MR compilation due to,
  // e.g., optimization
  public MultiMap<PhysicalOperator, PhysicalOperator> phyToMRMap;
  //
  private List<POPartition> outputPartitions = new ArrayList<POPartition>();

  private List<POPartition> inputPartitions = new ArrayList<POPartition>();

  private Map<String, List<PhysicalOperator>> partitionSeen = new HashMap<String, List<PhysicalOperator>>();

  // private Map<PhysicalOperator, String> partitionSeen = new
  // HashMap<PhysicalOperator, String>();

  public TopologyOperator(OperatorKey k) {
    super(k);
    topoPlan = new PhysicalPlan();
    UDFs = new HashSet<String>();
    scalars = new HashSet<PhysicalOperator>();
    nig = NodeIdGenerator.getGenerator();
    scope = k.getScope();
    phyToMRMap = new MultiMap<PhysicalOperator, PhysicalOperator>();
  }

  public boolean hasTap() {
    if (this.topoPlan == null || this.topoPlan.size() == 0) {
      return false;
    }

    for (PhysicalOperator root : this.topoPlan.getRoots()) {
      if (root instanceof POTap) {
        return true;
      }
    }

    return false;
  }

  public List<POPartition> getOutputPartitions() {
    return outputPartitions;
  }

  public List<POPartition> getInputPartitions() {
    return inputPartitions;
  }

  public void setPartitionSeen(Map<String, List<PhysicalOperator>> partitionSeen) {
    this.partitionSeen = partitionSeen;
  }

  /**
   * TODO form Partition to other PhysicalOperators
   * 
   * @param alias
   * @return
   */
  public Map<String, List<String>> getPartitionSuccessors() {
    Map<String, List<String>> all = new HashMap<String, List<String>>();
    for (POPartition po : inputPartitions) {
      if (partitionSeen.containsKey(po.getAlias())) {
        List<PhysicalOperator> tos = partitionSeen.get(po.getAlias());
        List<String> ret = new ArrayList<String>();
        for (PhysicalOperator to : tos) {
          ret.add(to.getAlias());
        }
        all.put(po.getAlias(), ret);
      }
    }
    return all;
  }

  private String shiftStringByTabs(String DFStr, String tab) {
    StringBuilder sb = new StringBuilder();
    String[] spl = DFStr.split("\n");
    for (int i = 0; i < spl.length; i++) {
      sb.append(tab);
      sb.append(spl[i]);
      sb.append("\n");
    }
    sb.delete(sb.length() - "\n".length(), sb.length());
    return sb.toString();
  }

  /**
   * Uses the string representation of the component plans to identify itself.
   */
  @Override
  public String name() {
    String udfStr = getUDFsAsStr();
    StringBuilder sb = new StringBuilder("Topology" + "("
        + requestedParallelism + (udfStr.equals("") ? "" : ",") + udfStr + ")"
        + " - " + mKey.toString() + ":\n");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    if (!topoPlan.isEmpty()) {
      topoPlan.explain(baos);
      String mp = new String(baos.toByteArray());
      sb.append(shiftStringByTabs(mp, "|   "));
    } else
      sb.append("Physical Plan Empty");
    return sb.toString();
  }

  private String getUDFsAsStr() {
    StringBuilder sb = new StringBuilder();
    if (UDFs != null && UDFs.size() > 0) {
      for (String str : UDFs) {
        sb.append(str.substring(str.lastIndexOf('.') + 1));
        sb.append(',');
      }
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  @Override
  public boolean supportsMultipleInputs() {
    return true;
  }

  @Override
  public boolean supportsMultipleOutputs() {
    return true;
  }

  @Override
  public void visit(TopologyOperatorPlanVisitor v) throws VisitorException {
    v.visitTopologyOperator(this);
  }

  public boolean isTopoDone() {
    return topoDone;
  }

  public void setTopoDone(boolean topoDone) {
    this.topoDone = topoDone;
  }

  public void setTopoDoneSingle(boolean topoDone) throws PlanException {
    this.topoDone = topoDone;
    if (topoDone && topoPlan.getLeaves().size() > 1) {
      topoPlan.addAsLeaf(getUnion());
    }
  }

  public void setTopoDoneMultiple(boolean topoDone) throws PlanException {
    this.topoDone = topoDone;
    if (topoDone && topoPlan.getLeaves().size() > 0) {
      topoPlan.addAsLeaf(getUnion());
    }
  }

  private POUnion getUnion() {
    return new POUnion(new OperatorKey(scope, nig.getNextNodeId(scope)));
  }

  public boolean isGlobalSort() {
    return globalSort;
  }

  public void setGlobalSort(boolean globalSort) {
    this.globalSort = globalSort;
  }

  public boolean isLimitAfterSort() {
    return limitAfterSort;
  }

  public void setLimitAfterSort(boolean las) {
    limitAfterSort = las;
  }

  public boolean isLimitOnly() {
    return limitOnly;
  }

  public void setLimitOnly(boolean limitOnly) {
    this.limitOnly = limitOnly;
  }

  public void setSortOrder(boolean[] sortOrder) {
    if (null == sortOrder)
      return;
    this.sortOrder = new boolean[sortOrder.length];
    for (int i = 0; i < sortOrder.length; ++i) {
      this.sortOrder[i] = sortOrder[i];
    }
  }

  public void setSecondarySortOrder(boolean[] secondarySortOrder) {
    if (null == secondarySortOrder)
      return;
    this.secondarySortOrder = new boolean[secondarySortOrder.length];
    for (int i = 0; i < secondarySortOrder.length; ++i) {
      this.secondarySortOrder[i] = secondarySortOrder[i];
    }
  }

  public boolean[] getSortOrder() {
    return sortOrder;
  }

  public boolean[] getSecondarySortOrder() {
    return secondarySortOrder;
  }

  /**
   * @return whether end of all input is set in the map plan
   */
  public boolean isEndOfAllInputSetInMap() {
    return endOfAllInputInMap;
  }

  /**
   * @param endOfAllInputInMap
   *          the streamInMap to set
   */
  public void setEndOfAllInputInMap(boolean endOfAllInputInMap) {
    this.endOfAllInputInMap = endOfAllInputInMap;
  }

  public int getRequestedParallelism() {
    return requestedParallelism;
  }

  public String getCustomPartitioner() {
    return customPartitioner;
  }

  public boolean isRankOperation() {
    return getRankOperationId().size() != 0;
  }

  public ArrayList<String> getRankOperationId() {
    ArrayList<String> operationIDs = new ArrayList<String>();
    Iterator<PhysicalOperator> mapRoots = this.topoPlan.getRoots().iterator();

    while (mapRoots.hasNext()) {
      PhysicalOperator operation = mapRoots.next();
      if (operation instanceof PORank)
        operationIDs.add(((PORank) operation).getOperationID());
    }

    return operationIDs;
  }

  public boolean isCounterOperation() {
    return (getCounterOperation() != null);
  }

  public boolean isRowNumber() {
    POCounter counter = getCounterOperation();
    return (counter != null) ? counter.isRowNumber() : false;
  }

  public String getOperationID() {
    POCounter counter = getCounterOperation();
    return (counter != null) ? counter.getOperationID() : null;
  }

  private POCounter getCounterOperation() {
    PhysicalOperator operator;
    Iterator<PhysicalOperator> it = this.topoPlan.getLeaves().iterator();

    while (it.hasNext()) {
      operator = it.next();
      if (operator instanceof POCounter)
        return (POCounter) operator;
    }

    return null;
  }
}
