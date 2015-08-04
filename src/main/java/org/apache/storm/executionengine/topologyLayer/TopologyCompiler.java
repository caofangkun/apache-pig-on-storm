package org.apache.storm.executionengine.topologyLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POBind;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POCross;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.PODump;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POGroup;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.PONative;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPackage.PackageType;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POTap;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.storm.executionengine.topologyLayer.plans.PartitionComposer;
import org.apache.storm.executionengine.topologyLayer.plans.PartitionTopologyChecker;
import org.apache.storm.executionengine.topologyLayer.plans.ScalarPhyFinder;
import org.apache.storm.executionengine.topologyLayer.plans.TopologyOperatorPlan;
import org.apache.storm.executionengine.topologyLayer.plans.UDFFinder;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * From PO to TO: The compiler that compiles a given physical plan into a DAG of
 * Topology operators which can then be converted into the Topology structure.
 * 
 * Uses a predecessor based depth first traversal. To compile an operator, first
 * compiles the predecessors into Topology Operators and tries to merge the
 * current operator into one of them. The goal being to keep the number of
 * Topology Opers to a minimum.
 * 
 * Only in case of POPartition, a new Topology operator is started. Whenever
 * this happens care is taken to add the TopologyOper into the TopologyPlan and
 * connect it appropriately.
 * 
 * 
 */
public class TopologyCompiler extends PhyPlanVisitor {

  PigContext pigContext;

  // The plan that is being compiled
  PhysicalPlan plan;

  // The plan of Topology Operators
  TopologyOperatorPlan topologyOperatorPlan;

  // The current Topology Operator
  // that is being compiled
  TopologyOperator curTopologyOperator;

  // The output of compiling the inputs
  TopologyOperator[] compiledInputs = null;

  Map<OperatorKey, TopologyOperator> partitionSeen;

  Map<OperatorKey, TopologyOperator> bindSeen;

  PartitionTopologyChecker partitionChecker;

  NodeIdGenerator nig;

  private String scope;

  private Random r;

  private UDFFinder udfFinder;

  private CompilationMessageCollector messageCollector = null;

  private Map<PhysicalOperator, TopologyOperator> phyToMROpMap;

  public static final String USER_COMPARATOR_MARKER = "user.comparator.func:";

  private static final Log LOG = LogFactory.getLog(TopologyCompiler.class);

  public static final String FILE_CONCATENATION_THRESHOLD = "pig.files.concatenation.threshold";

  public static final String OPTIMISTIC_FILE_CONCATENATION = "pig.optimistic.files.concatenation";

  private int fileConcatenationThreshold = 100;

  private boolean optimisticFileConcatenation = false;

  public TopologyCompiler(PhysicalPlan plan) throws TopologyCompilerException {
    this(plan, null);
  }

  public TopologyCompiler(PhysicalPlan plan, PigContext pigContext)
      throws TopologyCompilerException {
    //
    super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));

    this.plan = plan;
    this.pigContext = pigContext;
    partitionSeen = new HashMap<OperatorKey, TopologyOperator>();
    bindSeen = new HashMap<OperatorKey, TopologyOperator>();
    topologyOperatorPlan = new TopologyOperatorPlan();
    nig = NodeIdGenerator.getGenerator();
    r = new Random(1331);
    FileLocalizer.setR(r);
    udfFinder = new UDFFinder();
    List<PhysicalOperator> roots = plan.getRoots();
    if ((roots == null) || (roots.size() <= 0)) {
      int errCode = 2053;
      String msg = "Internal error. Did not find roots in the physical plan.";
      throw new TopologyCompilerException(msg, errCode, PigException.BUG);
    }
    scope = roots.get(0).getOperatorKey().getScope();
    messageCollector = new CompilationMessageCollector();
    phyToMROpMap = new HashMap<PhysicalOperator, TopologyOperator>();

    fileConcatenationThreshold = Integer.parseInt(pigContext.getProperties()
        .getProperty(FILE_CONCATENATION_THRESHOLD, "100"));
    optimisticFileConcatenation = pigContext.getProperties()
        .getProperty(OPTIMISTIC_FILE_CONCATENATION, "false").equals("true");
    LOG.info("File concatenation threshold: " + fileConcatenationThreshold
        + " optimistic? " + optimisticFileConcatenation);
  }

  /**
   * Used to get the compiled plan
   * 
   * @return TopologyOperatorPlan built by the compiler
   */
  public TopologyOperatorPlan getTopologyOperatorPlan() {
    return topologyOperatorPlan;
  }

  /**
   * Used to get the plan that was compiled
   * 
   * @return physical plan
   */
  @Override
  public PhysicalPlan getPlan() {
    return plan;
  }

  public CompilationMessageCollector getMessageCollector() {
    return messageCollector;
  }

  /**
   * TODO:The front-end method that the user calls to compile the plan.
   * 
   * @return A TopologyOperatorPlan plan
   * @throws IOException
   * @throws PlanException
   * @throws VisitorException
   */
  public TopologyOperatorPlan compile() throws PlanException, VisitorException {
    // PhysicalPlan
    new PartitionComposer(plan).visit();

    List<PhysicalOperator> leaves = plan.getLeaves();

    // sort them in order(operatorid) and compile their plans
    List<PhysicalOperator> ops = new ArrayList<PhysicalOperator>(leaves.size());
    ops.addAll(leaves);
    Collections.sort(ops);

    for (PhysicalOperator op : ops) {
      compile(op);// compile the leaves
    }

    connectSoftLink();
    // TopologyOperatorPlan
    partitionChecker = new PartitionTopologyChecker(topologyOperatorPlan);
    partitionChecker.visit();
    partitionChecker.reset();
    return topologyOperatorPlan;
  }

  public void connectSoftLink() throws PlanException {
    for (PhysicalOperator op : plan) {
      if (plan.getSoftLinkPredecessors(op) != null) {
        for (PhysicalOperator pred : plan.getSoftLinkPredecessors(op)) {
          TopologyOperator from = phyToMROpMap.get(pred);
          TopologyOperator to = phyToMROpMap.get(op);
          if (from == to)
            continue;
          if (topologyOperatorPlan.getPredecessors(to) == null
              || !topologyOperatorPlan.getPredecessors(to).contains(from)) {
            topologyOperatorPlan.connect(from, to);
          }
        }
      }
    }
  }

  /**
   * Compiles the plan below op into a TopologyOperator and stores it in
   * curTopologyOperator.
   * 
   * @param op
   * @throws IOException
   * @throws PlanException
   * @throws VisitorException
   */
  private void compile(PhysicalOperator op) throws PlanException,
      VisitorException {
    if (phyToMROpMap.containsKey(op)) {
      curTopologyOperator = phyToMROpMap.get(op);
      return;// recursion exit
    }

    // An artifact of the Visitor. Need to save
    // this so that it is not overwritten.
    TopologyOperator[] prevCompInp = compiledInputs;

    // Compile each predecessor into the TopologyOperator and
    // store them away so that we can use them for compiling
    // op.
    List<PhysicalOperator> predecessors = plan.getPredecessors(op);

    if (predecessors != null && predecessors.size() > 0) {
      // 2.
      Collections.sort(predecessors);
      compiledInputs = new TopologyOperator[predecessors.size()];
      //
      int i = -1;
      for (int k = 0; k < predecessors.size(); k++) {
        PhysicalOperator pred = predecessors.get(k);
        // for (PhysicalOperator pred : predecessors) {
        // POPartition startNewPartition
        if (pred instanceof POPartition
            && partitionSeen.containsKey(pred.getOperatorKey())) {
          // compiledInputs[++i] = startNewPartition((POPartition)
          // pred,partitionSeen.get(pred.getOperatorKey()));
          compiledInputs[++i] = phyToMROpMap.get(pred);
          continue;
        }
        // POBind startMultiplePartition
        if (pred instanceof POBind
            && bindSeen.containsKey(pred.getOperatorKey())) {
          // compiledInputs[++i] = startMultiplePartition((POBind)
          // pred,bindSeen.get(pred.getOperatorKey()));
          compiledInputs[++i] = phyToMROpMap.get(pred);
          continue;
        }
        compile(pred);// recursion call,until create the first TO for TAP.
        compiledInputs[++i] = curTopologyOperator;
      }
    } else {
      // 1.No predecessors. Mostly a tap. But this is where
      // we start. We create a new TopologyOperator and add its first
      // operator op. Also this should be added to the
      // topologyOperatorPlan.
      curTopologyOperator = getTopologyOperator();// new TO for TAP
      curTopologyOperator.topoPlan.add(op);// sub PP.

      if (op != null && op instanceof POTap) {
        if (((POTap) op).getFuncSpec() != null
            && ((POTap) op).getFuncSpec() != null)
          curTopologyOperator.UDFs.add(((POTap) op).getFuncSpec().toString());
      }

      topologyOperatorPlan.add(curTopologyOperator);// the TP.
      phyToMROpMap.put(op, curTopologyOperator);
      return;// recursion exit
    }

    // Now we have the inputs compiled.
    op.visit(this);// Do something with the input oper op.

    if (op.getRequestedParallelism() > curTopologyOperator.requestedParallelism) {
      curTopologyOperator.requestedParallelism = op.getRequestedParallelism();
    }

    compiledInputs = prevCompInp;
  }

  private TopologyOperator getTopologyOperator() {
    return new TopologyOperator(
        new OperatorKey(scope, nig.getNextNodeId(scope)));
  }

  /**
   * Push the input operator into the TO
   * 
   * @param topoOper
   * @param op
   * @throws PlanException
   * @throws IOException
   */
  private void appendPhysicalPlan(TopologyOperator topoOper, PhysicalOperator op)
      throws PlanException, IOException {
    List<PhysicalOperator> ret = new ArrayList<PhysicalOperator>();

    List<PhysicalOperator> inputs = op.getInputs();
    for (PhysicalOperator ipo : inputs) {
      PhysicalOperator po = topoOper.topoPlan.getOperator(ipo.getOperatorKey());
      if (po != null) {
        ret.add(po);
      }
    }

    topoOper.topoPlan.add(op);

    for (PhysicalOperator oper : ret) {
      topoOper.topoPlan.connect(oper, op);
    }
  }

  /**
   * Used for compiling non-blocking operators. The logic here is simple. If
   * there is a single input, just push the operator into whichever phase is
   * open. Otherwise, we merge the compiled inputs into a list of Topology
   * operators where the first oper is the merged oper consisting of all
   * Topology operators. Then we add the input oper op into the merged Topology
   * operator's plan as a leaf and connect the Topology operator to the input
   * operator which is the leaf. Also care is taken to connect the Topology
   * operators according to the dependencies.
   * 
   * @param op
   * @throws PlanException
   * @throws IOException
   */
  private void nonBlocking(PhysicalOperator op) throws PlanException,
      IOException {

    if (compiledInputs.length == 1) {
      // 1:n
      // For speed
      TopologyOperator topoOper = compiledInputs[0];
      if (!topoOper.isTopoDone()) {

        if (!(op instanceof POPartition) && !(op instanceof POBind)) {
          // Push the input operator into the TO
          // topoOper.topoPlan.addAsLeaf(op);
          appendPhysicalPlan(topoOper, op);
        }

      } else {
        int errCode = 2022;
        String msg = "Phases have been done. This is unexpected while compiling.";
        throw new PlanException(msg, errCode, PigException.BUG);
      }
      // return the compiled TO
      curTopologyOperator = topoOper;
    } else {
      // n:1
      // merge the TO
      // List<TopologyOperator> mergedPlans = merge(compiledInputs);
      List<TopologyOperator> mergedPlans = new ArrayList<TopologyOperator>();
      for (TopologyOperator to : compiledInputs) {
        mergedPlans.add(to);
      }
      // The first TO is always the merged TO
      TopologyOperator topoOper = mergedPlans.remove(0);

      if (!topoOper.isTopoDone()) {

        if (!(op instanceof POPartition) && !(op instanceof POBind)) {
          // Push the input operator into the TO
          // topoOper.topoPlan.addAsLeaf(op);
          appendPhysicalPlan(topoOper, op);
          // Connect all the rest TO
          if (mergedPlans.size() > 0)
            connectTopologyOperator(mergedPlans, topoOper);
        }

      } else {
        int errCode = 2022;
        String msg = "Phases have been done. This is unexpected while compiling.";
        throw new PlanException(msg, errCode, PigException.BUG);
      }

      // return the compiled TO
      curTopologyOperator = topoOper;
    }
  }

  private void addToMap(PhysicalOperator op) throws PlanException, IOException {

    if (compiledInputs.length == 1) {
      // For speed
      TopologyOperator topoOper = compiledInputs[0];
      if (!topoOper.isTopoDone()) {
        topoOper.topoPlan.addAsLeaf(op);
      } else {
        int errCode = 2022;
        String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";
        throw new PlanException(msg, errCode, PigException.BUG);
      }
      curTopologyOperator = topoOper;
    } else {
      List<TopologyOperator> mergedPlans = merge(compiledInputs);

      // The first MROper is always the merged map MROper
      TopologyOperator topoOper = mergedPlans.remove(0);
      // Push the input operator into the merged map MROper
      topoOper.topoPlan.addAsLeaf(op);

      // Connect all the reduce MROpers
      if (mergedPlans.size() > 0)
        connectTopologyOperator(mergedPlans, topoOper);

      // return the compiled MROper
      curTopologyOperator = topoOper;
    }
  }

  /**
   * Used for compiling blocking operators. If there is a single input and its
   * map phase is still open, then close it so that further operators can be
   * compiled into the reduce phase. If its reduce phase is open, add a store
   * and close it. Start a new map MROper into which further operators can be
   * compiled into.
   * 
   * If there are multiple inputs, the logic is to merge all map MROpers into
   * one map MROper and retain the reduce MROpers. Since the operator is
   * blocking, it has to be a Global Rerrange at least now. This operator need
   * not be inserted into our plan as it is implemented by hadoop. But this
   * creates the map-reduce boundary. So the merged map MROper is closed and its
   * reduce phase is started. Depending on the number of reduce MROpers and the
   * number of pipelines in the map MRoper a Union operator is inserted whenever
   * necessary. This also leads to the possibility of empty map plans. So have
   * to be careful while handling it in the PigMapReduce class. If there are no
   * map plans, then a new one is created as a side effect of the merge process.
   * If there are no reduce MROpers, and only a single pipeline in the map, then
   * no union oper is added. Otherwise a Union oper is added to the merged map
   * MROper to which all the reduce MROpers are connected by store-load
   * combinations. Care is taken to connect the MROpers in the
   * topologyOperatorPlan.
   * 
   * @param op
   * @throws IOException
   * @throws PlanException
   */
  private void blocking(PhysicalOperator op) throws IOException, PlanException {
    if (compiledInputs.length == 1) {
      TopologyOperator topoOper = compiledInputs[0];
      if (!topoOper.isTopoDone()) {
        topoOper.setTopoDoneSingle(true);
        curTopologyOperator = topoOper;
      }
    } else {
      List<TopologyOperator> mergedPlans = merge(compiledInputs);
      TopologyOperator topoOper = mergedPlans.remove(0);

      if (mergedPlans.size() > 0)
        topoOper.setTopoDoneMultiple(true);
      else
        topoOper.setTopoDoneSingle(true);

      // Connect all the reduce MROpers
      if (mergedPlans.size() > 0)
        connectTopologyOperator(mergedPlans, topoOper);
      curTopologyOperator = topoOper;
    }
  }

  /**
   * Connect the rest TopologyOperator to the leaf node in the prev
   * TopologyOperator by adding appropriate Partitions
   * 
   * @param mergedPlans
   *          - The list of rest TopologyOperator
   * @param to
   *          - The prev TopologyOperator
   * @throws PlanException
   * @throws IOException
   */
  private void connectTopologyOperator(List<TopologyOperator> mergedPlans,
      TopologyOperator to) throws PlanException, IOException {

    PhysicalOperator leaf = null;

    List<PhysicalOperator> leaves = to.topoPlan.getLeaves();
    if (leaves != null && leaves.size() > 0)
      leaf = leaves.get(0);

    for (TopologyOperator mto : mergedPlans) {

      // mto.setTopoDone(true);
      // //
      // POPartition ld = getPartition();
      // POPartition str = getPartition();
      // //
      // mto.topoPlan.addAsLeaf(str);// POPartition
      // to.topoPlan.add(ld);// POPartition

      if (leaf != null)
        // to.topoPlan.connect(ld, leaf);
        if (!to.equals(mto)) {
          topologyOperatorPlan.connect(mto, to);
        }
    }
  }

  /**
   * Starts a new topoOper and connects it to the old one.
   * 
   * @param fSpec
   * @param old
   * @return
   * @throws IOException
   * @throws PlanException
   * @throws CloneNotSupportedException
   */
  private TopologyOperator startNewPartition(POPartition fromPartition,
      TopologyOperator old) throws PlanException {
    try {
      TopologyOperator ret = null;
      if (fromPartition.isNewPartition()) {
        ret = getTopologyOperator();
        //
        POPartition toPartition = (POPartition) fromPartition.clone();
        //
        old.getOutputPartitions().add(fromPartition);// from
        ret.getInputPartitions().add(toPartition);// to
        //
        topologyOperatorPlan.add(ret);
        topologyOperatorPlan.connect(old, ret);
        ret.setPartitionSeen(fromPartition.getPartitionSeen());
      } else {
        ret = curTopologyOperator;
      }
      return ret;
    } catch (CloneNotSupportedException e) {
      throw new PlanException(e);
    }
  }

  /**
   * Starts a new topoOper and connects it to the old one.
   * 
   * @param fSpec
   * @param old
   * @return
   * @throws IOException
   * @throws PlanException
   * @throws CloneNotSupportedException
   */
  private TopologyOperator startMultiplePartition(POBind bind,
      TopologyOperator[] compiledTOs) throws PlanException {
    try {
      TopologyOperator ret = getTopologyOperator();
      topologyOperatorPlan.add(ret);

      List<PhysicalOperator> inputs = bind.getInputs();
      for (PhysicalOperator po : inputs) {
        if (po instanceof POPartition) {

          for (TopologyOperator old : compiledTOs) {
            List<PhysicalOperator> leaves = old.topoPlan.getLeaves();
            POPartition fromPartition = (POPartition) po;
            List<PhysicalOperator> aboves = fromPartition.getInputs();
            if (leaves.containsAll(aboves)) {
              POPartition toPartition = (POPartition) fromPartition.clone();
              if (!old.getOutputPartitions().contains(fromPartition)
                  && !ret.getInputPartitions().contains(toPartition)) {
                //
                old.getOutputPartitions().add(fromPartition);// from
                ret.getInputPartitions().add(toPartition);// to
                //
                topologyOperatorPlan.connect(old, ret);
              }

            }
          }
        }
      }
      //
      ret.setPartitionSeen(bind.getPartitionSeen());
      return ret;
    } catch (CloneNotSupportedException e) {
      throw new PlanException(e);
    }
  }

  /**
   * Merges the TOs in the compiledInputs into a single merged TO and returns a
   * List with the merged TO as the first oper.
   * 
   * 
   * Merge is implemented as a sequence of binary merges. merge(PhyPlan finPlan,
   * List<PhyPlan> lst) := finPlan,merge(p) foreach p in lst
   * 
   * @param compiledInputs
   * @return
   * @throws PlanException
   * @throws IOException
   */
  private List<TopologyOperator> merge(TopologyOperator[] compiledInputs)
      throws PlanException {
    List<TopologyOperator> ret = new ArrayList<TopologyOperator>();

    TopologyOperator mergedTO = getTopologyOperator();
    ret.add(mergedTO);
    topologyOperatorPlan.add(mergedTO);

    Set<TopologyOperator> toBeConnected = new HashSet<TopologyOperator>();
    List<TopologyOperator> remLst = new ArrayList<TopologyOperator>();

    List<PhysicalPlan> mpLst = new ArrayList<PhysicalPlan>();

    for (TopologyOperator compiledTO : compiledInputs) {
      if (!compiledTO.isTopoDone()) {
        remLst.add(compiledTO);
        mpLst.add(compiledTO.topoPlan);
        List<TopologyOperator> pmros = topologyOperatorPlan
            .getPredecessors(compiledTO);
        if (pmros != null) {
          for (TopologyOperator pmro : pmros)
            toBeConnected.add(pmro);
        }
      } else {
        ret.add(compiledTO);
      }
    }
    merge(ret.get(0).topoPlan, mpLst);

    Iterator<TopologyOperator> it = toBeConnected.iterator();
    while (it.hasNext())
      topologyOperatorPlan.connect(it.next(), mergedTO);
    for (TopologyOperator rmro : remLst) {
      if (rmro.requestedParallelism > mergedTO.requestedParallelism)
        mergedTO.requestedParallelism = rmro.requestedParallelism;
      for (String udf : rmro.UDFs) {
        if (!mergedTO.UDFs.contains(udf))
          mergedTO.UDFs.add(udf);
      }
      // We also need to change scalar marking
      for (PhysicalOperator physOp : rmro.scalars) {
        if (!mergedTO.scalars.contains(physOp)) {
          mergedTO.scalars.add(physOp);
        }
      }
      Set<PhysicalOperator> opsToChange = new HashSet<PhysicalOperator>();
      for (Map.Entry<PhysicalOperator, TopologyOperator> entry : phyToMROpMap
          .entrySet()) {
        if (entry.getValue() == rmro) {
          opsToChange.add(entry.getKey());
        }
      }
      for (PhysicalOperator op : opsToChange) {
        phyToMROpMap.put(op, mergedTO);
      }

      topologyOperatorPlan.remove(rmro);
    }
    return ret;
  }

  /**
   * The merge of a list of map plans
   * 
   * @param <O>
   * @param <E>
   * @param finPlan
   *          - Final Plan into which the list of plans is merged
   * @param plans
   *          - list of map plans to be merged
   * @throws PlanException
   */
  private <O extends Operator, E extends OperatorPlan<O>> void merge(E finPlan,
      List<E> plans) throws PlanException {
    for (E e : plans) {
      finPlan.merge(e);
    }
  }

  private void processUDFs(PhysicalPlan plan) throws VisitorException {
    if (plan != null) {
      // Process Scalars (UDF with referencedOperators)
      ScalarPhyFinder scalarPhyFinder = new ScalarPhyFinder(plan);
      scalarPhyFinder.visit();
      curTopologyOperator.scalars.addAll(scalarPhyFinder.getScalars());
      // Process UDFs
      udfFinder.setPlan(plan);
      udfFinder.visit();
      curTopologyOperator.UDFs.addAll(udfFinder.getUDFs());
    }
  }

  /* The visitOp methods that decide what to do with the current operator */

  @Override
  public void visitTap(POTap tap) throws VisitorException {
    try {
      // TODO the first TopologyOperator
      nonBlocking(tap);
      phyToMROpMap.put(tap, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + tap.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitLoad(POLoad op) throws VisitorException {

  }

  @Override
  public void visitNative(PONative op) throws VisitorException {

  }

  @Override
  public void visitStore(POStore op) throws VisitorException {

  }

  @Override
  public void visitDump(PODump dp) throws VisitorException {
    try {
      nonBlocking(dp);
      phyToMROpMap.put(dp, curTopologyOperator);
      if (dp.getFuncSpec() != null)
        curTopologyOperator.UDFs.add(dp.getFuncSpec().toString());
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + dp.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitPartition(POPartition pt) throws VisitorException {
    try {
      nonBlocking(pt);
      //
      List<PhysicalPlan> plans = pt.getPlans();
      if (plans != null)
        for (PhysicalPlan ep : plans)
          processUDFs(ep);
      //
      TopologyOperator old = compiledInputs[0];
      partitionSeen.put(pt.getOperatorKey(), old);
      curTopologyOperator = startNewPartition(pt, old);
      //
      phyToMROpMap.put(pt, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + pt.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitBind(POBind bi) throws VisitorException {
    try {
      nonBlocking(bi);
      //
      TopologyOperator old = compiledInputs[0];
      bindSeen.put(bi.getOperatorKey(), old);
      curTopologyOperator = startMultiplePartition(bi, compiledInputs);
      //
      phyToMROpMap.put(bi, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + bi.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitSplit(POSplit op) throws VisitorException {
    try {
      nonBlocking(op);
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitFilter(POFilter op) throws VisitorException {
    try {
      nonBlocking(op);
      processUDFs(op.getPlan());
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitGroup(POGroup op) throws VisitorException {
    if (op.getInputs().size() > 1 && needPartitionAndBind()) {
      int errCode = 2034;
      StringBuffer sb = new StringBuffer();
      sb.append("Alias:");
      for (String input : op.getInputAlias()) {
        sb.append(" ");
        sb.append(input);
      }
      sb.append(" Input for GROUP Operator at:");
      sb.append(op.getOriginalLocations().get(0).toString());
      sb.append(" has not been PARTITION and BIND.");
      throw new TopologyCompilerException(sb.toString(), errCode);
    }
    try {
      nonBlocking(op);
      for (List<PhysicalPlan> plans : op.getAllPlans()) {
        for (PhysicalPlan plan : plans) {
          processUDFs(plan);
        }
      }
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitCross(POCross op) throws VisitorException {
    try {
      nonBlocking(op);
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitStream(POStream op) throws VisitorException {
    try {
      nonBlocking(op);
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitLimit(POLimit op) throws VisitorException {
    try {
      nonBlocking(op);
      if (op.getLimitPlan() != null) {
        processUDFs(op.getLimitPlan());
      }
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitPOForEach(POForEach op) throws VisitorException {
    try {
      nonBlocking(op);
      List<PhysicalPlan> plans = op.getInputPlans();
      if (plans != null)
        for (PhysicalPlan plan : plans) {
          processUDFs(plan);
        }
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitLocalRearrange(POLocalRearrange op) throws VisitorException {
    try {
      addToMap(op);
      List<PhysicalPlan> plans = op.getPlans();
      if (plans != null)
        for (PhysicalPlan ep : plans)
          processUDFs(ep);
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitGlobalRearrange(POGlobalRearrange op)
      throws VisitorException {
    try {
      blocking(op);
      curTopologyOperator.customPartitioner = op.getCustomPartitioner();
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitPackage(POPackage op) throws VisitorException {
    try {
      nonBlocking(op);
      phyToMROpMap.put(op, curTopologyOperator);
      if (op.getPackageType() == PackageType.JOIN) {
        // curTopologyOperator.markRegularJoin();
      } else if (op.getPackageType() == PackageType.GROUP) {
        if (op.getNumInps() == 1) {
          // curTopologyOperator.markGroupBy();
        } else if (op.getNumInps() > 1) {
          // curTopologyOperator.markCogroup();
        }
      }
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  private boolean needPartitionAndBind() {
    Set<TopologyOperator> mergedPlans = new HashSet<TopologyOperator>();
    for (TopologyOperator to : compiledInputs) {
      mergedPlans.add(to);
    }
    if (mergedPlans.size() > 1) {
      // multi-TAP
      for (TopologyOperator to : mergedPlans) {
        if (to.getOutputPartitions().size() > 1) {
          return false;
        }
      }
    } else {
      // single-TAP
      return false;
    }
    return true;
  }

  @Override
  public void visitUnion(POUnion op) throws VisitorException {
    if (op.getInputs().size() > 1 && needPartitionAndBind()) {
      int errCode = 2034;
      StringBuffer sb = new StringBuffer();
      sb.append("Alias:");
      for (String input : op.getInputAlias()) {
        sb.append(" ");
        sb.append(input);
      }
      sb.append(" Input for UNION Operator at:");
      sb.append(op.getOriginalLocations().get(0).toString());
      sb.append(" has not been PARTITION and BIND.");
      throw new TopologyCompilerException(sb.toString(), errCode);
    }
    try {
      nonBlocking(op);
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  /**
   * This is an operator which will have multiple inputs(= to number of join
   * inputs) But it prunes off all inputs but the fragment input and creates
   * separate MR jobs for each of the replicated inputs and uses these as the
   * replicated files that are configured in the POFRJoin operator. It also sets
   * that this is FRJoin job and some parametes associated with it.
   */
  @Override
  public void visitFRJoin(POFRJoin op) throws VisitorException {

  }

  /**
   * Since merge-join works on two inputs there are exactly two MROper
   * predecessors identified as left and right. Instead of merging two
   * operators, both are used to generate a MR job each. First MR oper is run to
   * generate on-the-fly index on right side. Second is used to actually do the
   * join. First MR oper is identified as rightMROper and second as
   * curTopologyOperatorer.
   * 
   * 1) RightMROper: If it is in map phase. It can be preceded only by POLoad.
   * If there is anything else in physical plan, that is yanked and set as inner
   * plans of joinOp. If it is reduce phase. Close this operator and start new
   * MROper. 2) LeftMROper: If it is in map phase, add the Join operator in it.
   * If it is in reduce phase. Close it and start new MROper.
   */

  @Override
  public void visitMergeJoin(POMergeJoin joinOp) throws VisitorException {

  }

  @Override
  public void visitSkewedJoin(POSkewedJoin op) throws VisitorException {

  }

  /**
   * Leftmost relation is referred as base relation (this is the one fed into
   * mappers.) First, close all MROpers except for first one (referred as
   * baseMROPer) Then, create a MROper which will do indexing job (idxMROper)
   * Connect idxMROper before the mappedMROper in the topologyOperatorPlan.
   */

  @Override
  public void visitMergeCoGroup(POMergeCogroup poCoGrp) throws VisitorException {

  }

  @Override
  public void visitCollectedGroup(POCollectedGroup op) throws VisitorException {

  }

  @Override
  public void visitDistinct(PODistinct op) throws VisitorException {
    try {
      // blocking(op);
      nonBlocking(op);
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  @Override
  public void visitSort(POSort op) throws VisitorException {
    try {
      nonBlocking(op);
      if (op.isUDFComparatorUsed) {
        curTopologyOperator.UDFs
            .add(op.getMSortFunc().getFuncSpec().toString());
        curTopologyOperator.isUDFComparatorUsed = true;
      }
      phyToMROpMap.put(op, curTopologyOperator);
    } catch (Exception e) {
      int errCode = 2034;
      String msg = "Error compiling operator " + op.getClass().getSimpleName();
      throw new TopologyCompilerException(msg, errCode, PigException.BUG, e);
    }
  }

  /**
   * For the counter job, it depends if it is row number or not. In case of
   * being a row number, any previous jobs are saved and POCounter is added as a
   * leaf on a map task. If it is not, then POCounter is added as a leaf on a
   * reduce task (last sorting phase).
   **/
  @Override
  public void visitCounter(POCounter op) throws VisitorException {

  }

  /**
   * In case of PORank, it is closed any other previous job (containing
   * POCounter as a leaf) and PORank is added on map phase.
   **/
  @Override
  public void visitRank(PORank op) throws VisitorException {

  }

}
