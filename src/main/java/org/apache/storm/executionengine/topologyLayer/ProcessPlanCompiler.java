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

import org.apache.pig.PigException;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POBind;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POCross;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.PODump;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POGroup;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPackage.PackageType;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POTap;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

public class ProcessPlanCompiler extends PhyPlanVisitor {

	// The plan that is being compiled
	PhysicalPlan plan;

	// The plan of Topology Operators
	ProcessorOperatorPlan ProcessorOperatorPlan;

	// The current Topology Operator
	// that is being compiled
	ProcessorOperator curProcessorOperator;

	// The output of compiling the inputs
	ProcessorOperator[] compiledInputs = null;

	Map<OperatorKey, ProcessorOperator> windowSeen;

	NodeIdGenerator nig;

	private String scope;

	private Random r;

	private Map<OperatorKey, ProcessorOperator> phyToPROpMap;// PhysicalOperator

	public ProcessPlanCompiler(TopologyOperator to)
			throws TopologyCompilerException {
		//
		super(to.topoPlan,
				new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
						to.topoPlan));

		this.plan = to.topoPlan;
		windowSeen = new HashMap<OperatorKey, ProcessorOperator>();
		ProcessorOperatorPlan = new ProcessorOperatorPlan();
		nig = NodeIdGenerator.getGenerator();
		r = new Random(1331);
		FileLocalizer.setR(r);
		List<PhysicalOperator> roots = plan.getRoots();
		if ((roots == null) || (roots.size() <= 0)) {
			int errCode = 2053;
			String msg = "Internal error. Did not find roots in the physical plan.";
			throw new TopologyCompilerException(msg, errCode, PigException.BUG);
		}
		scope = roots.get(0).getOperatorKey().getScope();
		phyToPROpMap = new HashMap<OperatorKey, ProcessorOperator>();
	}

	/**
	 * Used to get the compiled plan
	 * 
	 * @return ProcessorOperatorPlan built by the compiler
	 */
	public ProcessorOperatorPlan getProcessorOperatorPlan() {
		return ProcessorOperatorPlan;
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

	/**
	 * The front-end method that the user calls to compile the plan.
	 * 
	 * @return A ProcessorOperatorPlan plan
	 * @throws IOException
	 * @throws PlanException
	 * @throws VisitorException
	 */
	public ProcessorOperatorPlan compile() throws PlanException,
			VisitorException {
		// PhysicalPlan
		// new WindowFinder(plan).visit();

		List<PhysicalOperator> leaves = plan.getLeaves();

		// sort them in order(operatorid) and compile their plans
		List<PhysicalOperator> ops = new ArrayList<PhysicalOperator>(
				leaves.size());
		ops.addAll(leaves);
		Collections.sort(ops);

		for (PhysicalOperator op : ops) {
			compile(op);// compile the leaves
		}
		// ProcessorOperatorPlan
		List<ProcessorOperator> leafPOS = new ArrayList<ProcessorOperator>();
		leafPOS.addAll(ProcessorOperatorPlan.getLeaves());
		Iterator<ProcessorOperator> pos = leafPOS.iterator();
		while (pos.hasNext()) {
			ProcessorOperator po = pos.next();
			if (po.getInnerPlan().isEmpty()) {
				ProcessorOperatorPlan.remove(po);
			}
		}

		connectSoftLink();

		// ProcessPlanIOSetter processPlanIOSetter = new ProcessPlanIOSetter(
		// ProcessorOperatorPlan);
		// processPlanIOSetter.visit();

		return ProcessorOperatorPlan;
	}

	public void connectSoftLink() throws PlanException {
		for (PhysicalOperator op : plan) {
			if (plan.getSoftLinkPredecessors(op) != null) {
				for (PhysicalOperator pred : plan.getSoftLinkPredecessors(op)) {
					ProcessorOperator from = phyToPROpMap.get(pred
							.getOperatorKey());
					ProcessorOperator to = phyToPROpMap
							.get(op.getOperatorKey());
					if (from == to)
						continue;
					if (ProcessorOperatorPlan.getPredecessors(to) == null
							|| !ProcessorOperatorPlan.getPredecessors(to)
									.contains(from)) {
						ProcessorOperatorPlan.connect(from, to);
					}
				}
			}
		}
	}

	/**
	 * Compiles the plan below op into a ProcessorOperator and stores it in
	 * curProcessorOperator.
	 * 
	 * @param op
	 * @throws IOException
	 * @throws PlanException
	 * @throws VisitorException
	 */
	private void compile(PhysicalOperator op) throws PlanException,
			VisitorException {
		if (phyToPROpMap.containsKey(op.getOperatorKey())) {
			curProcessorOperator = phyToPROpMap.get(op.getOperatorKey());
			return;// recursion exit
		}

		// An artifact of the Visitor. Need to save
		// this so that it is not overwritten.
		ProcessorOperator[] prevCompInp = compiledInputs;

		// Compile each predecessor into the ProcessorOperator and
		// store them away so that we can use them for compiling
		// op.
		List<PhysicalOperator> predecessors = plan.getPredecessors(op);

		if (predecessors != null && predecessors.size() > 0) {
			// 2.
			Collections.sort(predecessors);
			compiledInputs = new ProcessorOperator[predecessors.size()];
			//
			int i = -1;
			for (int k = 0; k < predecessors.size(); k++) {
				PhysicalOperator pred = predecessors.get(k);
				// for (PhysicalOperator pred : predecessors) {
				// POForEach startNewWindow
				if (pred instanceof POForEach
						&& windowSeen.containsKey(pred.getOperatorKey())) {
					compiledInputs[++i] = phyToPROpMap.get(pred
							.getOperatorKey());
					continue;
				} else if (pred instanceof POStream
						&& windowSeen.containsKey(pred.getOperatorKey())) {
					compiledInputs[++i] = phyToPROpMap.get(pred
							.getOperatorKey());
					continue;
				}
				compile(pred);// recursion call,until create the first TO for
								// TAP.
				compiledInputs[++i] = curProcessorOperator;
			}
		} else {
			// 1.No predecessors. Mostly a tap. But this is where
			// we start. We create a new ProcessorOperator and add its first
			// operator op. Also this should be added to the
			// ProcessorOperatorPlan.
			if (op instanceof POForEach) {
				POForEach fromWindow = (POForEach) op;
				if (fromWindow.isWindowMode()
						&& !plan.getLeaves().contains(fromWindow)) {
					curProcessorOperator = getProcessorOperator();// POForEach_Window
					curProcessorOperator.getInnerPlan().add(op);

					ProcessorOperator old = curProcessorOperator;
					old.setWindowProcessor(fromWindow.isWindowMode());
					ProcessorOperatorPlan.add(old);
					phyToPROpMap.put(op.getOperatorKey(), old);
				} else {
					if (curProcessorOperator == null
							|| !pathIntersected(op, curProcessorOperator
									.getInnerPlan().getRoots().get(0))) {
						curProcessorOperator = getProcessorOperator();// new TO
					}
					curProcessorOperator.getInnerPlan().add(op);
				}
			} else if (op instanceof POStream) {
				POStream fromWindow = (POStream) op;
				if (!plan.getLeaves().contains(fromWindow)) {
					curProcessorOperator = getProcessorOperator();// POStream
					curProcessorOperator.getInnerPlan().add(op);

					ProcessorOperator old = curProcessorOperator;
					old.setWindowProcessor(fromWindow.isWindowOperator());
					ProcessorOperatorPlan.add(old);
					phyToPROpMap.put(op.getOperatorKey(), old);
				} else {
					if (curProcessorOperator == null
							|| !pathIntersected(op, curProcessorOperator
									.getInnerPlan().getRoots().get(0))) {
						curProcessorOperator = getProcessorOperator();// new TO
					}
					curProcessorOperator.getInnerPlan().add(op);
				}

			} else {
				if (curProcessorOperator == null
						|| !pathIntersected(op, curProcessorOperator
								.getInnerPlan().getRoots().get(0))) {
					curProcessorOperator = getProcessorOperator();// TAP
				}
				curProcessorOperator.getInnerPlan().add(op);
			}
			//
			ProcessorOperatorPlan.add(curProcessorOperator);
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
			return;// recursion exit
		}

		// Now we have the inputs compiled.
		op.visit(this);// Do something with the input oper op.

		compiledInputs = prevCompInp;
	}

	private boolean pathIntersected(PhysicalOperator root1,
			PhysicalOperator root2) {
		List<PhysicalOperator> leaves = plan.getLeaves();
		// sort them in order(operatorid) and compile their plans
		List<PhysicalOperator> ops = new ArrayList<PhysicalOperator>(
				leaves.size());
		ops.addAll(leaves);
		Collections.sort(ops);
		//
		for (PhysicalOperator to : ops) {
			if (plan.pathExists(root1, to)) {
				if (plan.pathExists(root2, to)) {
					return true;
				}
			}
		}
		return false;
	}

	private ProcessorOperator getProcessorOperator() {
		return new ProcessorOperator(new OperatorKey(scope,
				nig.getNextNodeId(scope)), ProcessorOperatorPlan);
	}

	/**
	 * Push the input operator into the TO
	 * 
	 * @param prOper
	 * @param op
	 * @throws PlanException
	 * @throws IOException
	 */
	private void appendPhysicalPlan(ProcessorOperator prOper,
			PhysicalOperator op) throws PlanException, IOException {
		List<PhysicalOperator> ret = new ArrayList<PhysicalOperator>();

		List<PhysicalOperator> inputs = op.getInputs();
		for (PhysicalOperator ipo : inputs) {
			PhysicalOperator po = prOper.getInnerPlan().getOperator(
					ipo.getOperatorKey());
			if (po != null) {
				ret.add(po);
			}
		}

		prOper.getInnerPlan().add(op);

		for (PhysicalOperator oper : ret) {
			prOper.getInnerPlan().connect(oper, op);
		}
	}

	/**
	 * Used for compiling non-blocking operators. The logic here is simple. If
	 * there is a single input, just push the operator into whichever phase is
	 * open. Otherwise, we merge the compiled inputs into a list of Topology
	 * operators where the first oper is the merged oper consisting of all
	 * Topology operators. Then we add the input oper op into the merged
	 * Topology operator's plan as a leaf and connect the Topology operator to
	 * the input operator which is the leaf. Also care is taken to connect the
	 * Topology operators according to the dependencies.
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
			ProcessorOperator topoOper = compiledInputs[0];

			if (!(op instanceof POPartition) && !(op instanceof POBind)) {
				// Push the input operator into the TO
				// topoOper.topoPlan.addAsLeaf(op);
				appendPhysicalPlan(topoOper, op);
			}

			// return the compiled TO
			curProcessorOperator = topoOper;
		} else {
			// n:1
			// merge the TO
			// List<ProcessorOperator> mergedPlans = merge(compiledInputs);
			List<ProcessorOperator> mergedPlans = new ArrayList<ProcessorOperator>();
			for (ProcessorOperator to : compiledInputs) {
				mergedPlans.add(to);
			}
			// The first TO is always the merged TO ?
			ProcessorOperator topoOper = mergedPlans.remove(0);

			if (!(op instanceof POPartition) && !(op instanceof POBind)) {
				// Push the input operator into the TO
				// topoOper.topoPlan.addAsLeaf(op);
				appendPhysicalPlan(topoOper, op);
				// Connect all the rest TO
				if (mergedPlans.size() > 0)
					connectProcessorOperator(mergedPlans, topoOper);
			}

			// return the compiled TO
			curProcessorOperator = topoOper;
		}
	}

	/**
	 * Connect the rest ProcessorOperator to the leaf node in the prev
	 * ProcessorOperator by adding appropriate Partitions
	 * 
	 * @param mergedPlans
	 *            - The list of rest ProcessorOperator
	 * @param to
	 *            - The prev ProcessorOperator
	 * @throws PlanException
	 * @throws IOException
	 */
	private void connectProcessorOperator(List<ProcessorOperator> mergedPlans,
			ProcessorOperator to) throws PlanException, IOException {
		PhysicalOperator leaf = null;
		List<PhysicalOperator> leaves = to.getInnerPlan().getLeaves();
		if (leaves != null && leaves.size() > 0)
			leaf = leaves.get(0);
		for (ProcessorOperator mto : mergedPlans) {
			if (leaf != null && !mto.equals(to)) {
				to.putAllInputMap(mto.getInputMap());
				if (mto.getInnerPlan().isEmpty()) {
					ProcessorOperator prevMTO = ProcessorOperatorPlan
							.getPredecessors(mto).get(0);
					// TODO:ProcessorOperatorPlan.remove(mto);
					ProcessorOperatorPlan.connect(prevMTO, to);
				} else {
					//
					Map<String, Set<String>> fromToMap = new HashMap<String, Set<String>>();
					Set<String> targets = new HashSet<String>();
					List<PhysicalOperator> leavesMTO = mto.getInnerPlan()
							.getLeaves();
					for (PhysicalOperator leafMTO : leavesMTO) {
						List<PhysicalOperator> oldSucces = plan
								.getSuccessors(leafMTO);
						if (oldSucces != null) {
							for (PhysicalOperator po : oldSucces) {
								targets.add(po.getAlias());
							}
							fromToMap.put(leafMTO.getAlias(), targets);
							mto.putAllOutputMap(fromToMap);
						}
					}
					//
					to.putAllInputMap(mto.getOutputMap());
					if (ProcessorOperatorPlan.getOperatorKey(mto) != null) {
						ProcessorOperatorPlan.connect(mto, to);
					} else {
						mto.getInnerPlan();
					}
				}
			}
		}
	}

	private ProcessorOperator startNewWindow(PhysicalOperator fromWindow,
			ProcessorOperator old) throws PlanException {
		ProcessorOperator ret = null;

		if (old.getInnerPlan().isEmpty()) {
			ret = old;
		} else if (!old.getInnerPlan().hasOperator(fromWindow.getAlias())) {
			ret = getProcessorOperator();

			PhysicalOperator prevPO = plan.getPredecessors(fromWindow).get(0);
			if (!old.getInnerPlan().hasOperator(prevPO.getAlias())) {
				old = windowSeen.get(prevPO.getOperatorKey());
			}

			Map<String, Set<String>> fromToMap = new HashMap<String, Set<String>>();
			Set<String> targets = new HashSet<String>();
			targets.add(fromWindow.getAlias());
			fromToMap.put(prevPO.getAlias(), targets);
			// TODO:ret Input
			ret.putAllInputMap(fromToMap);
			//
			List<PhysicalOperator> leaves = old.getInnerPlan().getLeaves();
			for (PhysicalOperator leaf : leaves) {
				List<PhysicalOperator> oldSucces = plan.getSuccessors(leaf);
				if (oldSucces != null) {
					targets = new HashSet<String>();
					fromToMap = new HashMap<String, Set<String>>();
					for (PhysicalOperator po : oldSucces) {
						targets.add(po.getAlias());
					}
					fromToMap.put(leaf.getAlias(), targets);
					old.putAllOutputMap(fromToMap);
				}
			}
			//
			List<PhysicalOperator> succes = plan.getSuccessors(fromWindow);
			if (succes != null) {
				targets = new HashSet<String>();
				fromToMap = new HashMap<String, Set<String>>();
				for (PhysicalOperator po : succes) {
					targets.add(po.getAlias());
				}
				fromToMap.put(fromWindow.getAlias(), targets);
				// ret Output
				ret.putAllOutputMap(fromToMap);
			}

			ProcessorOperatorPlan.add(ret);
			ProcessorOperatorPlan.connect(old, ret);
		} else {
			if (old.getInnerPlan().size() == 1
					&& !plan.getLeaves().contains(fromWindow)) {
				ret = getProcessorOperator();
				// old Output
				if (old.getOutputMap().isEmpty()) {
					Map<String, Set<String>> fromToMap = new HashMap<String, Set<String>>();
					Set<String> targets = new HashSet<String>();

					List<PhysicalOperator> leaves = old.getInnerPlan()
							.getLeaves();
					for (PhysicalOperator leaf : leaves) {
						List<PhysicalOperator> oldSucces = plan
								.getSuccessors(leaf);
						if (oldSucces != null) {
							targets = new HashSet<String>();
							fromToMap = new HashMap<String, Set<String>>();
							for (PhysicalOperator po : oldSucces) {
								targets.add(po.getAlias());
							}
							fromToMap.put(leaf.getAlias(), targets);
							old.putAllOutputMap(fromToMap);
						}
					}
				}
				//
				ret.putAllInputMap(old.getOutputMap());

				ProcessorOperatorPlan.add(ret);
				ProcessorOperatorPlan.connect(old, ret);
			} else {
				ret = old;
			}
		}

		return ret;
	}

	private void processUDFs(PhysicalPlan plan) throws VisitorException {

	}

	/* The visitOp methods that decide what to do with the current operator */

	@Override
	public void visitTap(POTap tap) throws VisitorException {
		try {
			nonBlocking(tap);
			phyToPROpMap.put(tap.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ tap.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitDump(PODump dp) throws VisitorException {
		try {
			nonBlocking(dp);
			phyToPROpMap.put(dp.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ dp.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitSplit(POSplit op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitFilter(POFilter op) throws VisitorException {
		try {
			nonBlocking(op);
			processUDFs(op.getPlan());
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitGroup(POGroup op) throws VisitorException {
		try {
			nonBlocking(op);
			for (List<PhysicalPlan> plans : op.getAllPlans()) {
				for (PhysicalPlan plan : plans) {
					processUDFs(plan);
				}
			}
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitCross(POCross op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitStream(POStream op) throws VisitorException {
		try {
			ProcessorOperator old = compiledInputs[0];// get
			curProcessorOperator = startNewWindow(op, old);
			appendPhysicalPlan(curProcessorOperator, op);

			curProcessorOperator.setWindowProcessor(op.isWindowOperator());
			windowSeen.put(op.getOperatorKey(), curProcessorOperator);

			curProcessorOperator = startNewWindow(op, curProcessorOperator);
			compiledInputs[0] = curProcessorOperator;// set

			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitLimit(POLimit op) throws VisitorException {
		try {
			nonBlocking(op);
			if (op.getLimitPlan() != null) {
				processUDFs(op.getLimitPlan());
			}
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitPOForEach(POForEach op) throws VisitorException {
		try {
			// nonBlocking(op);
			List<PhysicalPlan> plans = op.getInputPlans();
			if (plans != null)
				for (PhysicalPlan plan : plans) {
					processUDFs(plan);
				}

			ProcessorOperator old = compiledInputs[0];

			curProcessorOperator = startNewWindow(op, old);
			appendPhysicalPlan(curProcessorOperator, op);// build inner
			curProcessorOperator.setWindowProcessor(op.isWindowMode());
			windowSeen.put(op.getOperatorKey(), curProcessorOperator);

			curProcessorOperator = startNewWindow(op, curProcessorOperator);
			compiledInputs[0] = curProcessorOperator;

			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitPackage(POPackage op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
			if (op.getPackageType() == PackageType.JOIN) {
				// curProcessorOperator.markRegularJoin();
			} else if (op.getPackageType() == PackageType.GROUP) {
				if (op.getNumInps() == 1) {
					// curProcessorOperator.markGroupBy();
				} else if (op.getNumInps() > 1) {
					// curProcessorOperator.markCogroup();
				}
			}
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitUnion(POUnion op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitDistinct(PODistinct op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

	@Override
	public void visitSort(POSort op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToPROpMap.put(op.getOperatorKey(), curProcessorOperator);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new TopologyCompilerException(msg, errCode, PigException.BUG,
					e);
		}
	}

}
