package org.apache.storm.executionengine.physicalLayer.compiler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POUserFunc;
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
import org.apache.pig.builtin.IsEmpty;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.data.SchemaTupleFrontend;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.GFCross;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.LinkedMultiMap;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalkerWOSeenChk;
import org.apache.pig.newplan.SubtreeDependencyOrderWalker;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOBind;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LODump;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LOPartition;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOTap;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.MapSideMergeValidator;
import org.apache.pig.parser.SourceLocation;

/**
 * From LO to PO.
 * 
 * @author ivangui
 * 
 */
public class LogicalRelationalOperatorCompiler extends
		LogicalRelationalNodesVisitor {

	public LogicalRelationalOperatorCompiler(OperatorPlan plan)
			throws FrontendException {
		super(plan, new DependencyOrderWalker(plan));
		currentPlan = new PhysicalPlan();
		logToPhyMap = new HashMap<Operator, PhysicalOperator>();
		currentPlans = new LinkedList<PhysicalPlan>();
	}

	protected Map<Operator, PhysicalOperator> logToPhyMap;

	protected Deque<PhysicalPlan> currentPlans;

	protected PhysicalPlan currentPlan;

	protected NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();

	protected PigContext pc;

	public void setPigContext(PigContext pc) {
		this.pc = pc;
	}

	public Map<Operator, PhysicalOperator> getLogToPhyMap() {
		return logToPhyMap;
	}

	public PhysicalPlan getPhysicalPlan() {
		return currentPlan;
	}

	@Override
	public void visit(LOTap loTap) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		// The last parameter here is set to true as we assume all files are
		// splittable due to LoadStore Refactor
		POTap tap = new POTap(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)), loTap.getTapFunc());
		tap.addOriginalLocation(loTap.getAlias(), loTap.getLocation());
		tap.setFuncSpec(loTap.getFuncSpec());
		tap.setContext(loTap.getContext());
		tap.setPc(pc);
		tap.setResultType(DataType.BAG);
		//
		tap.setLogicalSchema(loTap.getSchema());
		tap.setScriptSchema(loTap.getScriptSchema());
		tap.setRequestedParallelism(loTap.getRequestedParallelism());

		tap.setSignature(loTap.getSignature());
		tap.setLimit(loTap.getLimit());
		tap.setIsTmpLoad(loTap.isTmpLoad());

		currentPlan.add(tap);
		logToPhyMap.put(loTap, tap);

		// Load is typically a root operator, but in the multiquery
		// case it might have a store as a predecessor.
		List<Operator> op = loTap.getPlan().getPredecessors(loTap);
		PhysicalOperator from;

		if (op != null) {
			from = logToPhyMap.get(op.get(0));
			try {
				currentPlan.connect(from, tap);
			} catch (PlanException e) {
				int errCode = 2015;
				String msg = "Invalid physical operators in the physical plan";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, e);
			}
		}
	}

	@Override
	public void visit(LOLoad loLoad) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		// The last parameter here is set to true as we assume all files are
		// splittable due to LoadStore Refactor
		POLoad load = new POLoad(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)), loLoad.getLoadFunc());
		load.addOriginalLocation(loLoad.getAlias(), loLoad.getLocation());
		load.setLFile(loLoad.getFileSpec());
		load.setPc(pc);
		load.setResultType(DataType.BAG);
		load.setLogicalSchema(loLoad.getSchema());
		load.setSignature(loLoad.getSignature());
		load.setLimit(loLoad.getLimit());
		load.setIsTmpLoad(loLoad.isTmpLoad());

		currentPlan.add(load);
		logToPhyMap.put(loLoad, load);

		// Load is typically a root operator, but in the multiquery
		// case it might have a store as a predecessor.
		List<Operator> op = loLoad.getPlan().getPredecessors(loLoad);
		PhysicalOperator from;

		if (op != null) {
			from = logToPhyMap.get(op.get(0));
			try {
				currentPlan.connect(from, load);
			} catch (PlanException e) {
				int errCode = 2015;
				String msg = "Invalid physical operators in the physical plan";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, e);
			}
		}
	}

	@Override
	public void visit(LONative loNative) throws FrontendException {
		String scope = DEFAULT_SCOPE;

		PONative poNative = new PONative(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)));
		poNative.addOriginalLocation(loNative.getAlias(),
				loNative.getLocation());
		poNative.setNativeMRjar(loNative.getNativeMRJar());
		poNative.setParams(loNative.getParams());
		poNative.setResultType(DataType.BAG);
		poNative.setLogicalSchema(loNative.getSchema());
		logToPhyMap.put(loNative, poNative);
		currentPlan.add(poNative);

		List<Operator> op = loNative.getPlan().getPredecessors(loNative);

		PhysicalOperator from;
		if (op != null) {
			from = logToPhyMap.get(op.get(0));
		} else {
			int errCode = 2051;
			String msg = "Did not find a predecessor for Native.";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG);
		}

		try {
			currentPlan.connect(from, poNative);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

	}

	@Override
	public void visit(LOPartition partition) throws FrontendException {
		// can have only one input
		LogicalRelationalOperator pred = (LogicalRelationalOperator) plan
				.getPredecessors(partition).get(0);
		List<LogicalExpressionPlan> exprPlans = partition.getExpressionPlans()
				.get(0);
		POPartition physOp = new POPartition(new OperatorKey(DEFAULT_SCOPE,
				nodeGen.getNextNodeId(DEFAULT_SCOPE)));
		physOp.addOriginalLocation(partition.getAlias(),
				partition.getLocation());
		List<PhysicalPlan> pExprPlans = translateExpressionPlans(partition,
				exprPlans);

		physOp.setRequestedParallelism(partition.getRequestedParallelism());
		physOp.setLogicalSchema(partition.getSchema());
		try {
			physOp.setPlans(pExprPlans);
		} catch (PlanException pe) {
			int errCode = 2071;
			String msg = "Problem with setting up partition's plans.";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, pe);
		}
		Byte type = null;
		if (exprPlans.size() > 1) {
			type = DataType.TUPLE;
			physOp.setKeyType(type);
		} else {
			type = pExprPlans.get(0).getLeaves().get(0).getResultType();
			physOp.setKeyType(type);
		}
		physOp.setResultType(DataType.BAG);

		currentPlan.add(physOp);

		try {
			currentPlan.connect(logToPhyMap.get(pred), physOp);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

		logToPhyMap.put(partition, physOp);
		/**
     * 
     */
		translateSoftLinks(partition);
	}

	@Override
	public void visit(LOFilter filter) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		// System.err.println("Entering Filter");
		POFilter poFilter = new POFilter(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)), filter.getRequestedParallelism());
		poFilter.addOriginalLocation(filter.getAlias(), filter.getLocation());
		poFilter.setResultType(DataType.BAG);
		poFilter.setLogicalSchema(filter.getSchema());
		currentPlan.add(poFilter);
		logToPhyMap.put(filter, poFilter);
		currentPlans.push(currentPlan);

		currentPlan = new PhysicalPlan();

		// PlanWalker childWalker = currentWalker
		// .spawnChildWalker(filter.getFilterPlan());
		PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(
				filter.getFilterPlan());
		pushWalker(childWalker);
		// currentWalker.walk(this);
		currentWalker.walk(new LogicalExpressionCompiler(currentWalker
				.getPlan(), childWalker, filter, currentPlan, logToPhyMap));
		popWalker();

		poFilter.setPlan(currentPlan);
		currentPlan = currentPlans.pop();

		List<Operator> op = filter.getPlan().getPredecessors(filter);

		PhysicalOperator from;
		if (op != null) {
			from = logToPhyMap.get(op.get(0));
		} else {
			int errCode = 2051;
			String msg = "Did not find a predecessor for Filter.";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG);
		}

		try {
			currentPlan.connect(from, poFilter);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

		translateSoftLinks(filter);
		// System.err.println("Exiting Filter");
	}

	@Override
	public void visit(LOSort sort) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		List<LogicalExpressionPlan> logPlans = sort.getSortColPlans();
		List<PhysicalPlan> sortPlans = new ArrayList<PhysicalPlan>(
				logPlans.size());

		// convert all the logical expression plans to physical expression plans
		currentPlans.push(currentPlan);
		for (LogicalExpressionPlan plan : logPlans) {
			currentPlan = new PhysicalPlan();
			PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(
					plan);
			pushWalker(childWalker);
			childWalker.walk(new LogicalExpressionCompiler(currentWalker
					.getPlan(), childWalker, sort, currentPlan, logToPhyMap));
			sortPlans.add(currentPlan);
			popWalker();
		}
		currentPlan = currentPlans.pop();

		// get the physical operator for sort
		POSort poSort;
		if (sort.getUserFunc() == null) {
			poSort = new POSort(new OperatorKey(scope,
					nodeGen.getNextNodeId(scope)),
					sort.getRequestedParallelism(), null, sortPlans,
					sort.getAscendingCols(), null);
		} else {
			POUserComparisonFunc comparator = new POUserComparisonFunc(
					new OperatorKey(scope, nodeGen.getNextNodeId(scope)),
					sort.getRequestedParallelism(), null, sort.getUserFunc());
			poSort = new POSort(new OperatorKey(scope,
					nodeGen.getNextNodeId(scope)),
					sort.getRequestedParallelism(), null, sortPlans,
					sort.getAscendingCols(), comparator);
		}
		poSort.addOriginalLocation(sort.getAlias(), sort.getLocation());
		poSort.setLimit(sort.getLimit());
		poSort.setLogicalSchema(sort.getSchema());
		// sort.setRequestedParallelism(s.getType());
		logToPhyMap.put(sort, poSort);
		currentPlan.add(poSort);
		List<Operator> op = sort.getPlan().getPredecessors(sort);
		PhysicalOperator from;

		if (op != null) {
			from = logToPhyMap.get(op.get(0));
		} else {
			int errCode = 2051;
			String msg = "Did not find a predecessor for Sort.";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG);
		}

		try {
			currentPlan.connect(from, poSort);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

		poSort.setResultType(DataType.BAG);
	}

	/**
	 * Transformation from Logical to Physical Plan involves the following
	 * steps: First, it is generated a random number which will link a POCounter
	 * within a PORank. On this way, avoiding possible collisions on parallel
	 * rank operations. Then, if it is row number mode:
	 * 
	 * <pre>
	 * In case of a RANK operation (row number mode), are used two steps:
	 *   1.- Each tuple is counted sequentially on each mapper, and are produced global counters
	 *   2.- Global counters are gathered and summed, each tuple calls to the respective counter value
	 *       in order to calculate the corresponding rank value.
	 * </pre>
	 * 
	 * or not:
	 * 
	 * <pre>
	 * In case of a RANK BY operation, then are necessary five steps:
	 *   1.- Group by the fields involved on the rank operation: POPackage
	 *   2.- In case of multi-fields, the key (group field) is flatten: POForEach
	 *   3.- Sort operation by the fields available after flattening: POSort
	 *   4.- Each group is sequentially counted on each mapper through a global counter: POCounter
	 *   5.- Global counters are summed and passed to the rank operation: PORank
	 * </pre>
	 * 
	 * @param loRank
	 *            describe if the rank operation is on a row number mode or is
	 *            rank by (dense or not)
	 **/
	@Override
	public void visit(LORank loRank) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		PORank poRank;
		POCounter poCounter;

		Random randomGenerator = new Random();
		Long operationID = Math.abs(randomGenerator.nextLong());

		try {
			// Physical operations for RANK operator:
			// In case of a RANK BY operation, then are necessary five steps:
			// 1.- Group by the fields involved on the rank operation: POPackage
			// 2.- In case of multi-fields, the key (group field) is flatten:
			// POForEach
			// 3.- Sort operation by the fields available after flattening:
			// POSort
			// 4.- Each group is sequentially counted on each mapper through a
			// global counter: POCounter
			// 5.- Global counters are summed and passed to the rank operation:
			// PORank
			if (!loRank.isRowNumber()) {

				boolean[] flags = { false };

				MultiMap<Integer, LogicalExpressionPlan> expressionPlans = new MultiMap<Integer, LogicalExpressionPlan>();
				for (int i = 0; i < loRank.getRankColPlans().size(); i++)
					expressionPlans.put(i, loRank.getRankColPlans());

				POPackage poPackage = compileToLR_GR_PackTrio(loRank, null,
						flags, expressionPlans);
				poPackage.setPackageType(PackageType.GROUP);
				translateSoftLinks(loRank);

				List<Boolean> flattenLst = Arrays.asList(true, false);

				PhysicalPlan fep1 = new PhysicalPlan();
				POProject feproj1 = new POProject(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1);
				feproj1.addOriginalLocation(loRank.getAlias(),
						loRank.getLocation());
				feproj1.setColumn(0);
				feproj1.setResultType(poPackage.getKeyType());
				feproj1.setStar(false);
				feproj1.setOverloaded(false);
				fep1.add(feproj1);

				PhysicalPlan fep2 = new PhysicalPlan();
				POProject feproj2 = new POProject(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1);
				feproj2.addOriginalLocation(loRank.getAlias(),
						loRank.getLocation());
				feproj2.setColumn(1);
				feproj2.setResultType(DataType.BAG);
				feproj2.setStar(false);
				feproj2.setOverloaded(false);
				fep2.add(feproj2);
				List<PhysicalPlan> fePlans = Arrays.asList(fep1, fep2);

				POForEach poForEach = new POForEach(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1, fePlans, flattenLst);

				List<LogicalExpressionPlan> rankPlans = loRank
						.getRankColPlans();
				byte[] newTypes = new byte[rankPlans.size()];

				for (int i = 0; i < rankPlans.size(); i++) {
					LogicalExpressionPlan loep = rankPlans.get(i);
					Iterator<Operator> inpOpers = loep.getOperators();

					while (inpOpers.hasNext()) {
						Operator oper = inpOpers.next();
						newTypes[i] = ((ProjectExpression) oper).getType();
					}
				}

				List<PhysicalPlan> newPhysicalPlan = new ArrayList<PhysicalPlan>();
				List<Boolean> newOrderPlan = new ArrayList<Boolean>();

				for (int i = 0; i < loRank.getRankColPlans().size(); i++) {
					PhysicalPlan fep3 = new PhysicalPlan();
					POProject feproj3 = new POProject(new OperatorKey(scope,
							nodeGen.getNextNodeId(scope)), -1);
					feproj3.addOriginalLocation(loRank.getAlias(),
							loRank.getLocation());
					feproj3.setColumn(i);
					feproj3.setResultType(newTypes[i]);
					feproj3.setStar(false);
					feproj3.setOverloaded(false);
					fep3.add(feproj3);

					newPhysicalPlan.add(fep3);
					newOrderPlan.add(loRank.getAscendingCol().get(i));
				}

				POSort poSort;
				poSort = new POSort(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1, null,
						newPhysicalPlan, newOrderPlan, null);
				poSort.addOriginalLocation(loRank.getAlias(),
						loRank.getLocation());

				poCounter = new POCounter(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1, null,
						newPhysicalPlan, newOrderPlan);

				poCounter.addOriginalLocation(loRank.getAlias(),
						loRank.getLocation());
				poCounter.setResultType(DataType.TUPLE);
				poCounter.setIsRowNumber(loRank.isRowNumber());
				poCounter.setIsDenseRank(loRank.isDenseRank());
				poCounter.setOperationID(String.valueOf(operationID));

				poRank = new PORank(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1, null,
						newPhysicalPlan, newOrderPlan);

				poRank.addOriginalLocation(loRank.getAlias(),
						loRank.getLocation());
				poRank.setResultType(DataType.TUPLE);
				poRank.setOperationID(String.valueOf(operationID));

				List<Boolean> flattenLst2 = Arrays.asList(false, true);

				PhysicalPlan fep12 = new PhysicalPlan();
				POProject feproj12 = new POProject(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1);
				feproj12.addOriginalLocation(loRank.getAlias(),
						loRank.getLocation());
				feproj12.setColumn(0);
				feproj12.setResultType(DataType.LONG);
				feproj12.setStar(false);
				feproj12.setOverloaded(false);
				fep12.add(feproj12);

				PhysicalPlan fep22 = new PhysicalPlan();
				POProject feproj22 = new POProject(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1);
				feproj22.addOriginalLocation(loRank.getAlias(),
						loRank.getLocation());
				feproj22.setColumn(loRank.getRankColPlans().size() + 1);
				feproj22.setResultType(DataType.BAG);
				feproj22.setStar(false);
				feproj22.setOverloaded(false);
				fep22.add(feproj22);
				List<PhysicalPlan> fePlans2 = Arrays.asList(fep12, fep22);

				POForEach poForEach2 = new POForEach(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1, fePlans2,
						flattenLst2);

				currentPlan.add(poForEach);
				currentPlan.add(poSort);
				currentPlan.add(poCounter);
				currentPlan.add(poRank);
				currentPlan.add(poForEach2);

				try {
					currentPlan.connect(poPackage, poForEach);
					currentPlan.connect(poForEach, poSort);
					currentPlan.connect(poSort, poCounter);
					currentPlan.connect(poCounter, poRank);
					currentPlan.connect(poRank, poForEach2);
				} catch (PlanException e) {
					throw new LogicalToPhysicalTranslatorException(
							e.getMessage(), e.getErrorCode(),
							e.getErrorSource(), e);
				}

				logToPhyMap.put(loRank, poForEach2);

				// In case of a RANK operation, are used two steps:
				// 1.- Each tuple is counted sequentially on each mapper, and
				// are produced global counters
				// 2.- Global counters are gathered and summed, each tuple calls
				// to the respective counter value
				// in order to calculate the corresponding rank value.
			} else {

				List<LogicalExpressionPlan> logPlans = loRank.getRankColPlans();
				List<PhysicalPlan> rankPlans = new ArrayList<PhysicalPlan>(
						logPlans.size());

				// convert all the logical expression plans to physical
				// expression plans
				currentPlans.push(currentPlan);
				for (LogicalExpressionPlan plan : logPlans) {
					currentPlan = new PhysicalPlan();
					PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(
							plan);
					pushWalker(childWalker);
					childWalker.walk(new LogicalExpressionCompiler(
							currentWalker.getPlan(), childWalker, loRank,
							currentPlan, logToPhyMap));
					rankPlans.add(currentPlan);
					popWalker();
				}
				currentPlan = currentPlans.pop();

				poCounter = new POCounter(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1, null, rankPlans,
						loRank.getAscendingCol());

				poCounter.addOriginalLocation(loRank.getAlias(),
						loRank.getLocation());
				poCounter.setResultType(DataType.TUPLE);
				poCounter.setIsRowNumber(loRank.isRowNumber());
				poCounter.setIsDenseRank(loRank.isDenseRank());
				poCounter.setOperationID(String.valueOf(operationID));

				poRank = new PORank(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), -1, null, rankPlans,
						loRank.getAscendingCol());

				poRank.addOriginalLocation(loRank.getAlias(),
						loRank.getLocation());
				poRank.setResultType(DataType.TUPLE);
				poRank.setOperationID(String.valueOf(operationID));

				currentPlan.add(poCounter);
				currentPlan.add(poRank);

				List<Operator> op = loRank.getPlan().getPredecessors(loRank);
				PhysicalOperator from;

				if (op != null) {
					from = logToPhyMap.get(op.get(0));
				} else {
					int errCode = 2051;
					String msg = "Did not find a predecessor for Rank.";
					throw new LogicalToPhysicalTranslatorException(msg,
							errCode, PigException.BUG);
				}

				currentPlan.connect(from, poCounter);
				currentPlan.connect(poCounter, poRank);

				logToPhyMap.put(loRank, poRank);
			}

		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

	}

	@Override
	public void visit(LOCross cross) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		List<Operator> inputs = cross.getPlan().getPredecessors(cross);
		if (cross.isNested()) {
			POCross physOp = new POCross(new OperatorKey(scope,
					nodeGen.getNextNodeId(scope)),
					cross.getRequestedParallelism());
			physOp.addOriginalLocation(physOp.getAlias(),
					physOp.getOriginalLocations());
			currentPlan.add(physOp);
			physOp.setResultType(DataType.BAG);
			logToPhyMap.put(cross, physOp);
			for (Operator op : cross.getPlan().getPredecessors(cross)) {
				PhysicalOperator from = logToPhyMap.get(op);
				try {
					currentPlan.connect(from, physOp);
				} catch (PlanException e) {
					int errCode = 2015;
					String msg = "Invalid physical operators in the physical plan";
					throw new LogicalToPhysicalTranslatorException(msg,
							errCode, PigException.BUG, e);
				}
			}
		} else {
			POGlobalRearrange poGlobal = new POGlobalRearrange(new OperatorKey(
					scope, nodeGen.getNextNodeId(scope)),
					cross.getRequestedParallelism());
			poGlobal.addOriginalLocation(cross.getAlias(), cross.getLocation());
			POPackage poPackage = new POPackage(new OperatorKey(scope,
					nodeGen.getNextNodeId(scope)),
					cross.getRequestedParallelism());
			poGlobal.addOriginalLocation(cross.getAlias(), cross.getLocation());
			currentPlan.add(poGlobal);
			currentPlan.add(poPackage);

			int count = 0;

			try {
				currentPlan.connect(poGlobal, poPackage);
				List<Boolean> flattenLst = Arrays.asList(true, true);

				for (Operator op : inputs) {
					PhysicalPlan fep1 = new PhysicalPlan();
					ConstantExpression ce1 = new ConstantExpression(
							new OperatorKey(scope, nodeGen.getNextNodeId(scope)),
							cross.getRequestedParallelism());
					ce1.setValue(inputs.size());
					ce1.setResultType(DataType.INTEGER);
					fep1.add(ce1);

					ConstantExpression ce2 = new ConstantExpression(
							new OperatorKey(scope, nodeGen.getNextNodeId(scope)),
							cross.getRequestedParallelism());
					ce2.setValue(count);
					ce2.setResultType(DataType.INTEGER);
					fep1.add(ce2);
					/*
					 * Tuple ce1val = TupleFactory.getInstance().newTuple(2);
					 * ce1val.set(0,inputs.size()); ce1val.set(1,count);
					 * ce1.setValue(ce1val); ce1.setResultType(DataType.TUPLE);
					 */

					POUserFunc gfc = new POUserFunc(new OperatorKey(scope,
							nodeGen.getNextNodeId(scope)),
							cross.getRequestedParallelism(), Arrays.asList(
									(PhysicalOperator) ce1,
									(PhysicalOperator) ce2), new FuncSpec(
									GFCross.class.getName()));
					gfc.addOriginalLocation(cross.getAlias(),
							cross.getLocation());
					gfc.setResultType(DataType.BAG);
					fep1.addAsLeaf(gfc);
					gfc.setInputs(Arrays.asList((PhysicalOperator) ce1,
							(PhysicalOperator) ce2));
					/*
					 * fep1.add(gfc); fep1.connect(ce1, gfc); fep1.connect(ce2,
					 * gfc);
					 */

					PhysicalPlan fep2 = new PhysicalPlan();
					POProject feproj = new POProject(new OperatorKey(scope,
							nodeGen.getNextNodeId(scope)),
							cross.getRequestedParallelism());
					feproj.addOriginalLocation(cross.getAlias(),
							cross.getLocation());
					feproj.setResultType(DataType.TUPLE);
					feproj.setStar(true);
					feproj.setOverloaded(false);
					fep2.add(feproj);
					List<PhysicalPlan> fePlans = Arrays.asList(fep1, fep2);

					POForEach fe = new POForEach(new OperatorKey(scope,
							nodeGen.getNextNodeId(scope)),
							cross.getRequestedParallelism(), fePlans,
							flattenLst);
					fe.addOriginalLocation(cross.getAlias(),
							cross.getLocation());
					currentPlan.add(fe);
					currentPlan.connect(logToPhyMap.get(op), fe);

					POLocalRearrange physOp = new POLocalRearrange(
							new OperatorKey(scope, nodeGen.getNextNodeId(scope)),
							cross.getRequestedParallelism());
					physOp.addOriginalLocation(cross.getAlias(),
							cross.getLocation());
					List<PhysicalPlan> lrPlans = new ArrayList<PhysicalPlan>();
					for (int i = 0; i < inputs.size(); i++) {
						PhysicalPlan lrp1 = new PhysicalPlan();
						POProject lrproj1 = new POProject(new OperatorKey(
								scope, nodeGen.getNextNodeId(scope)),
								cross.getRequestedParallelism(), i);
						lrproj1.addOriginalLocation(cross.getAlias(),
								cross.getLocation());
						lrproj1.setOverloaded(false);
						lrproj1.setResultType(DataType.INTEGER);
						lrp1.add(lrproj1);
						lrPlans.add(lrp1);
					}

					physOp.setCross(true);
					physOp.setIndex(count++);
					physOp.setKeyType(DataType.TUPLE);
					physOp.setPlans(lrPlans);
					physOp.setResultType(DataType.TUPLE);

					currentPlan.add(physOp);
					currentPlan.connect(fe, physOp);
					currentPlan.connect(physOp, poGlobal);
				}
			} catch (PlanException e1) {
				int errCode = 2015;
				String msg = "Invalid physical operators in the physical plan";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, e1);
			} catch (ExecException e) {
				int errCode = 2058;
				String msg = "Unable to set index on newly create POLocalRearrange.";
				throw new VisitorException(msg, errCode, PigException.BUG, e);
			}

			poPackage.setKeyType(DataType.TUPLE);
			poPackage.setResultType(DataType.TUPLE);
			poPackage.setNumInps(count);
			boolean inner[] = new boolean[count];
			for (int i = 0; i < count; i++) {
				inner[i] = true;
			}
			poPackage.setInner(inner);

			List<PhysicalPlan> fePlans = new ArrayList<PhysicalPlan>();
			List<Boolean> flattenLst = new ArrayList<Boolean>();
			for (int i = 1; i <= count; i++) {
				PhysicalPlan fep1 = new PhysicalPlan();
				POProject feproj1 = new POProject(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)),
						cross.getRequestedParallelism(), i);
				feproj1.addOriginalLocation(cross.getAlias(),
						cross.getLocation());
				feproj1.setResultType(DataType.BAG);
				feproj1.setOverloaded(false);
				fep1.add(feproj1);
				fePlans.add(fep1);
				flattenLst.add(true);
			}

			POForEach fe = new POForEach(new OperatorKey(scope,
					nodeGen.getNextNodeId(scope)),
					cross.getRequestedParallelism(), fePlans, flattenLst);
			fe.addOriginalLocation(cross.getAlias(), cross.getLocation());
			currentPlan.add(fe);
			try {
				currentPlan.connect(poPackage, fe);
			} catch (PlanException e1) {
				int errCode = 2015;
				String msg = "Invalid physical operators in the physical plan";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, e1);
			}
			logToPhyMap.put(cross, fe);
		}
	}

	@Override
	public void visit(LOStream stream) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		POStream poStream = new POStream(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)), stream.getExecutableManager(),
				stream.getStreamingCommand(), this.pc);
		poStream.addOriginalLocation(stream.getAlias(), stream.getLocation());
		poStream.setLogicalSchema(stream.getSchema());
		currentPlan.add(poStream);
		logToPhyMap.put(stream, poStream);

		List<Operator> op = stream.getPlan().getPredecessors(stream);

		PhysicalOperator from;
		if (op != null) {
			from = logToPhyMap.get(op.get(0));
		} else {
			int errCode = 2051;
			String msg = "Did not find a predecessor for Stream.";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG);
		}

		try {
			currentPlan.connect(from, poStream);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}
	}

	@Override
	public void visit(LOInnerLoad load) throws FrontendException {
		String scope = DEFAULT_SCOPE;

		POProject exprOp = new POProject(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)));

		LogicalSchema s = load.getSchema();

		if (load.sourceIsBag()) {
			exprOp.setResultType(DataType.BAG);
			exprOp.setOverloaded(true);
		} else {
			if (s != null)
				exprOp.setResultType(s.getField(0).type);
			else
				exprOp.setResultType(DataType.BYTEARRAY);
		}

		ProjectExpression proj = load.getProjection();
		if (proj.isProjectStar()) {
			exprOp.setStar(proj.isProjectStar());
		} else if (proj.isRangeProject()) {
			if (proj.getEndCol() != -1) {
				// all other project-range should have been expanded by
				// project-star expander
				throw new AssertionError(
						"project range that is not a "
								+ "project-to-end seen in translation to physical plan!");
			}
			exprOp.setProjectToEnd(proj.getStartCol());
		} else {
			exprOp.setColumn(load.getColNum());
		}
		// set input to POProject to the predecessor of foreach

		logToPhyMap.put(load, exprOp);
		currentPlan.add(exprOp);
	}

	@Override
	public void visit(LOForEach foreach) throws FrontendException {
		String scope = DEFAULT_SCOPE;

		List<PhysicalPlan> innerPlans = new ArrayList<PhysicalPlan>();

		org.apache.pig.newplan.logical.relational.LogicalPlan inner = foreach
				.getInnerPlan();
		LOGenerate gen = (LOGenerate) inner.getSinks().get(0);

		List<LogicalExpressionPlan> exps = gen.getOutputPlans();
		List<Operator> preds = inner.getPredecessors(gen);

		currentPlans.push(currentPlan);

		// we need to translate each predecessor of LOGenerate into a physical
		// plan.
		// The physical plan should contain the expression plan for this
		// predecessor plus
		// the subtree starting with this predecessor
		for (int i = 0; i < exps.size(); i++) {
			currentPlan = new PhysicalPlan();
			// translate the expression plan
			PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(
					exps.get(i));
			pushWalker(childWalker);
			childWalker.walk(new LogicalExpressionCompiler(exps.get(i),
					childWalker, gen, currentPlan, logToPhyMap));
			popWalker();

			List<Operator> leaves = exps.get(i).getSinks();
			for (Operator l : leaves) {
				PhysicalOperator op = logToPhyMap.get(l);
				if (l instanceof ProjectExpression) {
					int input = ((ProjectExpression) l).getInputNum();

					// for each sink projection, get its input logical plan and
					// translate it
					Operator pred = preds.get(input);
					childWalker = new SubtreeDependencyOrderWalker(inner, pred);
					pushWalker(childWalker);
					childWalker.walk(this);
					popWalker();

					// get the physical operator of the leaf of input logical
					// plan
					PhysicalOperator leaf = logToPhyMap.get(pred);

					if (pred instanceof LOInnerLoad) {
						// if predecessor is only an LOInnerLoad, remove the
						// project that
						// comes from LOInnerLoad and change the column of
						// project that
						// comes from expression plan
						currentPlan.remove(leaf);
						logToPhyMap.remove(pred);

						POProject leafProj = (POProject) leaf;
						try {
							if (leafProj.isStar()) {
								((POProject) op).setStar(true);
							} else if (leafProj.isProjectToEnd()) {
								((POProject) op).setProjectToEnd(leafProj
										.getStartCol());
							} else {
								((POProject) op)
										.setColumn(leafProj.getColumn());
							}

						} catch (ExecException e) {
							throw new FrontendException(foreach,
									"Cannot get column from " + leaf, 2230, e);
						}

					} else {
						currentPlan.connect(leaf, op);
					}
				}
			}
			innerPlans.add(currentPlan);
		}

		currentPlan = currentPlans.pop();

		// PhysicalOperator poGen = new POGenerate(new OperatorKey("",
		// r.nextLong()), inputs, toBeFlattened);
		boolean[] flatten = gen.getFlattenFlags();
		List<Boolean> flattenList = new ArrayList<Boolean>();
		for (boolean fl : flatten) {
			flattenList.add(fl);
		}
		LogicalSchema logSchema = foreach.getSchema();
		Schema schema = null;
		if (logSchema != null) {
			try {
				schema = Schema.getPigSchema(new ResourceSchema(logSchema));
			} catch (FrontendException e) {
				throw new RuntimeException(
						"LogicalSchema in foreach unable to be converted to Schema: "
								+ logSchema, e);
			}
		}
		if (schema != null) {
			SchemaTupleFrontend.registerToGenerateIfPossible(schema, false,
					GenContext.FOREACH); // TODO may need to be appendable
		}
		POForEach poFE = new POForEach(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)),
				foreach.getRequestedParallelism(), innerPlans, flattenList,
				schema);
		poFE.addOriginalLocation(foreach.getAlias(), foreach.getLocation());
		poFE.setWindowMode(foreach.isWindowMode());
		poFE.setInterval(foreach.getInterval());
		poFE.setResultType(DataType.BAG);
		logToPhyMap.put(foreach, poFE);
		currentPlan.add(poFE);

		// generate cannot have multiple inputs
		List<Operator> inputs = foreach.getPlan().getPredecessors(foreach);

		// generate may not have any predecessors
		if (inputs == null)
			return;
		// TODO:foreach.isWindowMode()
		if (foreach.isWindowMode()) {
			List<List<PhysicalPlan>> allPlans = new ArrayList<List<PhysicalPlan>>();
			for (int i = 0; i < inputs.size(); i++) {
				// Operator op = inputs.get(i);
				// currentPlan.connect(logToPhyMap.get(op), poFE);
				List<LogicalExpressionPlan> plans = foreach
						.getExpressionPlans().get(i);
				List<PhysicalPlan> exprPlans = translateExpressionPlans(
						foreach, plans);
				allPlans.add(exprPlans);
			}
			poFE.setAllPlans(allPlans);
		}
		//
		PhysicalOperator from = logToPhyMap.get(inputs.get(0));
		try {
			currentPlan.connect(from, poFE);
		} catch (Exception e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

		translateSoftLinks(foreach);
	}

	/**
	 * This function takes in a List of LogicalExpressionPlan and converts them
	 * to a list of PhysicalPlans
	 * 
	 * @param plans
	 * @return
	 * @throws FrontendException
	 */
	private List<PhysicalPlan> translateExpressionPlans(
			LogicalRelationalOperator loj, List<LogicalExpressionPlan> plans)
			throws FrontendException {
		List<PhysicalPlan> exprPlans = new ArrayList<PhysicalPlan>();
		if (plans == null || plans.size() == 0) {
			return exprPlans;
		}

		// Save the current plan onto stack
		currentPlans.push(currentPlan);

		for (LogicalExpressionPlan lp : plans) {
			currentPlan = new PhysicalPlan();

			// We spawn a new Dependency Walker and use it
			// PlanWalker childWalker = currentWalker.spawnChildWalker(lp);
			PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(
					lp);

			// Save the old walker and use childWalker as current Walker
			pushWalker(childWalker);

			// We create a new ExpToPhyTranslationVisitor to walk the
			// ExpressionPlan
			currentWalker.walk(new LogicalExpressionCompiler(currentWalker
					.getPlan(), childWalker, loj, currentPlan, logToPhyMap));

			exprPlans.add(currentPlan);
			popWalker();
		}

		// Pop the current plan back out
		currentPlan = currentPlans.pop();

		return exprPlans;
	}

	@Override
	public void visit(LOStore loStore) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		// System.err.println("Entering Store");
		POStore store = new POStore(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)));
		store.addOriginalLocation(loStore.getAlias(), loStore.getLocation());
		store.setSFile(loStore.getOutputSpec());
		store.setInputSpec(loStore.getInputSpec());
		store.setSignature(loStore.getSignature());
		store.setSortInfo(loStore.getSortInfo());
		store.setIsTmpStore(loStore.isTmpStore());
		store.setStoreFunc(loStore.getStoreFunc());

		store.setSchema(Util.translateSchema(loStore.getSchema()));

		currentPlan.add(store);

		List<Operator> op = loStore.getPlan().getPredecessors(loStore);
		PhysicalOperator from = null;

		if (op != null) {
			from = logToPhyMap.get(op.get(0));
			// TODO Implement sorting when we have a LOSort (new) and LOLimit
			// (new) operator ready
			// SortInfo sortInfo = null;
			// // if store's predecessor is limit,
			// // check limit's predecessor
			// if(op.get(0) instanceof LOLimit) {
			// op = loStore.getPlan().getPredecessors(op.get(0));
			// }
			// PhysicalOperator sortPhyOp = logToPhyMap.get(op.get(0));
			// // if this predecessor is a sort, get
			// // the sort info.
			// if(op.get(0) instanceof LOSort) {
			// sortInfo = ((POSort)sortPhyOp).getSortInfo();
			// }
			// store.setSortInfo(sortInfo);
			// } else {
			// int errCode = 2051;
			// String msg = "Did not find a predecessor for Store." ;
			// throw new LogicalToPhysicalTranslatorException(msg, errCode,
			// PigException.BUG);
		}

		try {
			currentPlan.connect(from, store);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}
		logToPhyMap.put(loStore, store);
		// System.err.println("Exiting Store");
	}

	@Override
	public void visit(LODump loDump) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		// System.err.println("Entering Dump");
		PODump dump = new PODump(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)));
		dump.addOriginalLocation(loDump.getAlias(), loDump.getLocation());
		dump.setFuncSpec(loDump.getFuncSpec());// TODO
		dump.setInputSpec(loDump.getInputSpec());
		dump.setSignature(loDump.getSignature());
		dump.setSortInfo(loDump.getSortInfo());
		dump.setIsTmpStore(loDump.isTmpStore());
		dump.setDumpFunc(loDump.getDumpFunc());

		dump.setSchema(Util.translateSchema(loDump.getSchema()));

		currentPlan.add(dump);

		List<Operator> op = loDump.getPlan().getPredecessors(loDump);
		PhysicalOperator from = null;

		if (op != null) {
			from = logToPhyMap.get(op.get(0));
		}

		try {
			currentPlan.connect(from, dump);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}
		logToPhyMap.put(loDump, dump);
		// System.err.println("Exiting Dump");
	}

	@Override
	public void visit(LOCogroup cg) throws FrontendException {
		POGroup poGroup = compileToPO(cg, cg.getExpressionPlans());
		poGroup.setLogicalSchema(cg.getSchema());
		logToPhyMap.put(cg, poGroup);
		translateSoftLinks(cg);
	}

	private POGroup compileToPO(LogicalRelationalOperator relationalOp,
			MultiMap<Integer, LogicalExpressionPlan> innerPlans)
			throws FrontendException {
		POGroup poGroup = new POGroup(new OperatorKey(DEFAULT_SCOPE,
				nodeGen.getNextNodeId(DEFAULT_SCOPE)));
		poGroup.addOriginalLocation(relationalOp.getAlias(),
				relationalOp.getLocation());
		poGroup.setResultType(DataType.BAG);
		currentPlan.add(poGroup);

		List<Operator> inputs = relationalOp.getPlan().getPredecessors(
				relationalOp);
		List<List<PhysicalPlan>> allPlans = new ArrayList<List<PhysicalPlan>>();
		for (int i = 0; i < inputs.size(); i++) {
			Operator op = inputs.get(i);
			currentPlan.connect(logToPhyMap.get(op), poGroup);
			List<LogicalExpressionPlan> plans = innerPlans.get(i);
			List<PhysicalPlan> exprPlans = translateExpressionPlans(
					relationalOp, plans);
			allPlans.add(exprPlans);
		}
		poGroup.setAllPlans(allPlans);
		return poGroup;
	}

	public void visit1(LOCogroup cg) throws FrontendException {
		switch (cg.getGroupType()) {
		case COLLECTED:
			translateCollectedCogroup(cg);
			break;
		case REGULAR:
			POPackage poPackage = compileToLR_GR_PackTrio(cg,
					cg.getCustomPartitioner(), cg.getInner(),
					cg.getExpressionPlans());
			poPackage.setPackageType(PackageType.GROUP);
			logToPhyMap.put(cg, poPackage);
			break;
		case MERGE:
			translateMergeCogroup(cg);
			break;
		default:
			throw new LogicalToPhysicalTranslatorException(
					"Unknown CoGroup Modifier", PigException.BUG);
		}
		translateSoftLinks(cg);
	}

	private void translateCollectedCogroup(LOCogroup cg)
			throws FrontendException {
		// can have only one input
		LogicalRelationalOperator pred = (LogicalRelationalOperator) plan
				.getPredecessors(cg).get(0);
		List<LogicalExpressionPlan> exprPlans = cg.getExpressionPlans().get(0);
		POCollectedGroup physOp = new POCollectedGroup(new OperatorKey(
				DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
		physOp.addOriginalLocation(cg.getAlias(), cg.getLocation());
		List<PhysicalPlan> pExprPlans = translateExpressionPlans(cg, exprPlans);

		try {
			physOp.setPlans(pExprPlans);
		} catch (PlanException pe) {
			int errCode = 2071;
			String msg = "Problem with setting up map group's plans.";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, pe);
		}
		Byte type = null;
		if (exprPlans.size() > 1) {
			type = DataType.TUPLE;
			physOp.setKeyType(type);
		} else {
			type = pExprPlans.get(0).getLeaves().get(0).getResultType();
			physOp.setKeyType(type);
		}
		physOp.setResultType(DataType.TUPLE);

		currentPlan.add(physOp);

		try {
			currentPlan.connect(logToPhyMap.get(pred), physOp);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

		logToPhyMap.put(cg, physOp);
	}

	private POMergeCogroup compileToMergeCogrp(
			LogicalRelationalOperator relationalOp,
			MultiMap<Integer, LogicalExpressionPlan> innerPlans)
			throws FrontendException {

		List<Operator> inputs = relationalOp.getPlan().getPredecessors(
				relationalOp);
		// LocalRearrange corresponding to each of input
		// LR is needed to extract keys out of the tuples.

		POLocalRearrange[] innerLRs = new POLocalRearrange[inputs.size()];
		int count = 0;
		List<PhysicalOperator> inpPOs = new ArrayList<PhysicalOperator>(
				inputs.size());

		for (int i = 0; i < inputs.size(); i++) {
			Operator op = inputs.get(i);
			PhysicalOperator physOp = logToPhyMap.get(op);
			inpPOs.add(physOp);

			List<LogicalExpressionPlan> plans = innerPlans.get(i);
			POLocalRearrange poInnerLR = new POLocalRearrange(new OperatorKey(
					DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
			poInnerLR.addOriginalLocation(relationalOp.getAlias(),
					relationalOp.getLocation());
			// LR will contain list of physical plans, because there could be
			// multiple keys and each key can be an expression.
			List<PhysicalPlan> exprPlans = translateExpressionPlans(
					relationalOp, plans);
			try {
				poInnerLR.setPlans(exprPlans);
			} catch (PlanException pe) {
				int errCode = 2071;
				String msg = "Problem with setting up local rearrange's plans.";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, pe);
			}
			innerLRs[count] = poInnerLR;
			try {
				poInnerLR.setIndex(count++);
			} catch (ExecException e1) {
				int errCode = 2058;
				String msg = "Unable to set index on newly create POLocalRearrange.";
				throw new VisitorException(msg, errCode, PigException.BUG, e1);
			}
			poInnerLR.setKeyType(plans.size() > 1 ? DataType.TUPLE : exprPlans
					.get(0).getLeaves().get(0).getResultType());
			poInnerLR.setResultType(DataType.TUPLE);
		}

		POMergeCogroup poCogrp = new POMergeCogroup(new OperatorKey(
				DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)), inpPOs,
				innerLRs, relationalOp.getRequestedParallelism());
		return poCogrp;
	}

	private void translateMergeCogroup(LOCogroup cg) throws FrontendException {
		if (!validateMergeCogrp(cg.getInner())) {
			throw new LogicalToPhysicalTranslatorException("Inner is not "
					+ "supported for any relation on Merge Cogroup.");
		}
		List<Operator> inputs = cg.getPlan().getPredecessors(cg);
		MapSideMergeValidator validator = new MapSideMergeValidator();
		validator.validateMapSideMerge(inputs, cg.getPlan());
		POMergeCogroup poCogrp = compileToMergeCogrp(cg,
				cg.getExpressionPlans());
		poCogrp.setResultType(DataType.TUPLE);
		poCogrp.addOriginalLocation(cg.getAlias(), cg.getLocation());
		currentPlan.add(poCogrp);
		for (Operator op : inputs) {
			try {
				currentPlan.connect(logToPhyMap.get(op), poCogrp);
			} catch (PlanException e) {
				int errCode = 2015;
				String msg = "Invalid physical operators in the physical plan";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, e);
			}
		}
		logToPhyMap.put(cg, poCogrp);
	}

	private boolean validateMergeCogrp(boolean[] innerFlags) {

		for (boolean flag : innerFlags) {
			if (flag)
				return false;
		}
		return true;
	}

	@Override
	public void visit(LOJoin loj) throws FrontendException {

		String scope = DEFAULT_SCOPE;

		// List of join predicates
		List<Operator> inputs = loj.getPlan().getPredecessors(loj);

		// mapping of inner join physical plans corresponding to inner physical
		// operators.
		MultiMap<PhysicalOperator, PhysicalPlan> joinPlans = new LinkedMultiMap<PhysicalOperator, PhysicalPlan>();

		// Outer list corresponds to join predicates. Inner list is inner
		// physical plan of each predicate.
		List<List<PhysicalPlan>> ppLists = new ArrayList<List<PhysicalPlan>>();

		// List of physical operator corresponding to join predicates.
		List<PhysicalOperator> inp = new ArrayList<PhysicalOperator>();

		// Outer list corresponds to join predicates and inner list corresponds
		// to type of keys for each predicate.
		List<List<Byte>> keyTypes = new ArrayList<List<Byte>>();

		boolean[] innerFlags = loj.getInnerFlags();
		String alias = loj.getAlias();
		SourceLocation location = loj.getLocation();
		int parallel = loj.getRequestedParallelism();

		for (int i = 0; i < inputs.size(); i++) {
			Operator op = inputs.get(i);
			PhysicalOperator physOp = logToPhyMap.get(op);
			inp.add(physOp);
			List<LogicalExpressionPlan> plans = (List<LogicalExpressionPlan>) loj
					.getJoinPlan(i);

			List<PhysicalPlan> exprPlans = translateExpressionPlans(loj, plans);

			ppLists.add(exprPlans);
			joinPlans.put(physOp, exprPlans);

			// Key could potentially be a tuple. So, we visit all exprPlans to
			// get types of members of tuples.
			List<Byte> tupleKeyMemberTypes = new ArrayList<Byte>();
			for (PhysicalPlan exprPlan : exprPlans)
				tupleKeyMemberTypes.add(exprPlan.getLeaves().get(0)
						.getResultType());
			keyTypes.add(tupleKeyMemberTypes);
		}

		if (loj.getJoinType() == LOJoin.JOINTYPE.SKEWED) {
			POSkewedJoin skj;
			try {
				skj = new POSkewedJoin(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), parallel, inp,
						innerFlags);
				skj.addOriginalLocation(alias, location);
				skj.setJoinPlans(joinPlans);
			} catch (Exception e) {
				int errCode = 2015;
				String msg = "Skewed Join creation failed";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, e);
			}
			skj.setResultType(DataType.TUPLE);

			for (int i = 0; i < inputs.size(); i++) {
				Operator op = inputs.get(i);
				if (!innerFlags[i]) {
					try {
						LogicalSchema s = ((LogicalRelationalOperator) op)
								.getSchema();
						// if the schema cannot be determined
						if (s == null) {
							throw new FrontendException(loj,
									"Cannot determine skewed join schema", 2247);
						}
						skj.addSchema(Util.translateSchema(s));
					} catch (FrontendException e) {
						int errCode = 2015;
						String msg = "Couldn't set the schema for outer join";
						throw new LogicalToPhysicalTranslatorException(msg,
								errCode, PigException.BUG, e);
					}
				} else {
					// This will never be retrieved. It just guarantees that the
					// index will be valid when
					// MRCompiler is trying to read the schema
					skj.addSchema(null);
				}
			}

			currentPlan.add(skj);

			for (Operator op : inputs) {
				try {
					currentPlan.connect(logToPhyMap.get(op), skj);
				} catch (PlanException e) {
					int errCode = 2015;
					String msg = "Invalid physical operators in the physical plan";
					throw new LogicalToPhysicalTranslatorException(msg,
							errCode, PigException.BUG, e);
				}
			}
			logToPhyMap.put(loj, skj);
		} else if (loj.getJoinType() == LOJoin.JOINTYPE.REPLICATED) {
			Schema[] inputSchemas = new Schema[inputs.size()];
			Schema[] keySchemas = new Schema[inputs.size()];

			outer: for (int i = 0; i < inputs.size(); i++) {
				LogicalSchema logicalSchema = ((LogicalRelationalOperator) inputs
						.get(i)).getSchema();
				if (logicalSchema == null) {
					continue;
				}
				Schema toGen = Schema.getPigSchema(new ResourceSchema(
						logicalSchema));
				// This registers the value piece
				SchemaTupleFrontend.registerToGenerateIfPossible(toGen, false,
						GenContext.FR_JOIN);
				inputSchemas[i] = toGen;

				Schema keyToGen = new Schema();
				for (Byte byt : keyTypes.get(i)) {
					// We cannot generate any nested code because that
					// information is thrown away
					if (byt == null || DataType.isComplex(byt.byteValue())) {
						continue outer;
					}
					keyToGen.add(new FieldSchema(null, byt));
				}

				SchemaTupleFrontend.registerToGenerateIfPossible(keyToGen,
						false, GenContext.FR_JOIN);
				keySchemas[i] = keyToGen;
			}

			int fragment = 0;
			POFRJoin pfrj;
			try {
				boolean isLeftOuter = false;
				// We dont check for bounds issue as we assume that a join
				// involves atleast two inputs
				isLeftOuter = !innerFlags[1];

				Tuple nullTuple = null;
				if (isLeftOuter) {
					try {
						// We know that in a Left outer join its only a two way
						// join, so we assume index of 1 for the right input
						LogicalSchema inputSchema = ((LogicalRelationalOperator) inputs
								.get(1)).getSchema();

						// We check if we have a schema before the join
						if (inputSchema == null) {
							int errCode = 1109;
							String msg = "Input ("
									+ ((LogicalRelationalOperator) inputs
											.get(1)).getAlias()
									+ ") "
									+ "on which outer join is desired should have a valid schema";
							throw new LogicalToPhysicalTranslatorException(msg,
									errCode, PigException.INPUT);
						}

						// Using the schema we decide the number of
						// columns/fields
						// in the nullTuple
						nullTuple = TupleFactory.getInstance().newTuple(
								inputSchema.size());
						for (int j = 0; j < inputSchema.size(); j++) {
							nullTuple.set(j, null);
						}

					} catch (FrontendException e) {
						int errCode = 2104;
						String msg = "Error while determining the schema of input";
						throw new LogicalToPhysicalTranslatorException(msg,
								errCode, PigException.BUG, e);
					}
				}

				pfrj = new POFRJoin(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), parallel, inp, ppLists,
						keyTypes, null, fragment, isLeftOuter, nullTuple,
						inputSchemas, keySchemas);
				pfrj.addOriginalLocation(alias, location);
			} catch (ExecException e1) {
				int errCode = 2058;
				String msg = "Unable to set index on newly create POLocalRearrange.";
				throw new VisitorException(msg, errCode, PigException.BUG, e1);
			}
			pfrj.setResultType(DataType.TUPLE);
			currentPlan.add(pfrj);
			for (Operator op : inputs) {
				try {
					currentPlan.connect(logToPhyMap.get(op), pfrj);
				} catch (PlanException e) {
					int errCode = 2015;
					String msg = "Invalid physical operators in the physical plan";
					throw new LogicalToPhysicalTranslatorException(msg,
							errCode, PigException.BUG, e);
				}
			}
			logToPhyMap.put(loj, pfrj);
		} else if ((loj.getJoinType() == LOJoin.JOINTYPE.MERGE || loj
				.getJoinType() == LOJoin.JOINTYPE.MERGESPARSE)
				&& (new MapSideMergeValidator().validateMapSideMerge(inputs,
						loj.getPlan()))) {

			PhysicalOperator smj;
			boolean usePOMergeJoin = inputs.size() == 2 && innerFlags[0]
					&& innerFlags[1];

			if (usePOMergeJoin) {
				// We register the merge join schema information for code
				// generation
				LogicalSchema logicalSchema = ((LogicalRelationalOperator) inputs
						.get(0)).getSchema();
				Schema leftSchema = null;
				if (logicalSchema != null) {
					leftSchema = Schema.getPigSchema(new ResourceSchema(
							logicalSchema));
				}
				logicalSchema = ((LogicalRelationalOperator) inputs.get(1))
						.getSchema();
				Schema rightSchema = null;
				if (logicalSchema != null) {
					rightSchema = Schema.getPigSchema(new ResourceSchema(
							logicalSchema));
				}
				logicalSchema = loj.getSchema();
				Schema mergedSchema = null;
				if (logicalSchema != null) {
					mergedSchema = Schema.getPigSchema(new ResourceSchema(
							logicalSchema));
				}

				if (leftSchema != null) {
					SchemaTupleFrontend.registerToGenerateIfPossible(
							leftSchema, false, GenContext.MERGE_JOIN);
				}
				if (rightSchema != null) {
					SchemaTupleFrontend.registerToGenerateIfPossible(
							rightSchema, false, GenContext.MERGE_JOIN);
				}
				if (mergedSchema != null) {
					SchemaTupleFrontend.registerToGenerateIfPossible(
							mergedSchema, false, GenContext.MERGE_JOIN);
				}

				// inner join on two sorted inputs. We have less restrictive
				// implementation here in a form of POMergeJoin which doesn't
				// require loaders to implement collectable interface.
				try {
					smj = new POMergeJoin(new OperatorKey(scope,
							nodeGen.getNextNodeId(scope)), parallel, inp,
							joinPlans, keyTypes, loj.getJoinType(), leftSchema,
							rightSchema, mergedSchema);
				} catch (PlanException e) {
					int errCode = 2042;
					String msg = "Merge Join creation failed";
					throw new LogicalToPhysicalTranslatorException(msg,
							errCode, PigException.BUG, e);
				}
				logToPhyMap.put(loj, smj);
			} else {
				// in all other cases we fall back to POMergeCogroup +
				// Flattening FEs
				smj = compileToMergeCogrp(loj, loj.getExpressionPlans());
			}

			smj.setResultType(DataType.TUPLE);
			currentPlan.add(smj);
			smj.addOriginalLocation(alias, location);
			for (Operator op : inputs) {
				try {
					currentPlan.connect(logToPhyMap.get(op), smj);
				} catch (PlanException e) {
					int errCode = 2015;
					String msg = "Invalid physical operators in the physical plan";
					throw new LogicalToPhysicalTranslatorException(msg,
							errCode, PigException.BUG, e);
				}
			}

			if (!usePOMergeJoin) {
				// Now create and configure foreach which will flatten the
				// output
				// of cogroup.
				POForEach fe = compileFE4Flattening(innerFlags, scope,
						parallel, alias, location, inputs);
				currentPlan.add(fe);
				try {
					currentPlan.connect(smj, fe);
				} catch (PlanException e) {
					throw new LogicalToPhysicalTranslatorException(
							e.getMessage(), e.getErrorCode(),
							e.getErrorSource(), e);
				}
				logToPhyMap.put(loj, fe);
			}

			return;
		} else if (loj.getJoinType() == LOJoin.JOINTYPE.HASH) {
			POPackage poPackage = compileToLR_GR_PackTrio(loj,
					loj.getCustomPartitioner(), innerFlags,
					loj.getExpressionPlans());
			POForEach fe = compileFE4Flattening(innerFlags, scope, parallel,
					alias, location, inputs);
			currentPlan.add(fe);
			try {
				currentPlan.connect(poPackage, fe);
			} catch (PlanException e) {
				throw new LogicalToPhysicalTranslatorException(
						e.getDetailedMessage(), e.getErrorCode(),
						e.getErrorSource(), e);
			}
			logToPhyMap.put(loj, fe);
			poPackage.setPackageType(POPackage.PackageType.JOIN);
		}
		translateSoftLinks(loj);
	}

	private POPackage compileToLR_GR_PackTrio(
			LogicalRelationalOperator relationalOp, String customPartitioner,
			boolean[] innerFlags,
			MultiMap<Integer, LogicalExpressionPlan> innerPlans)
			throws FrontendException {

		POGlobalRearrange poGlobal = new POGlobalRearrange(new OperatorKey(
				DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)),
				relationalOp.getRequestedParallelism());
		poGlobal.addOriginalLocation(relationalOp.getAlias(),
				relationalOp.getLocation());
		poGlobal.setCustomPartitioner(customPartitioner);
		POPackage poPackage = new POPackage(new OperatorKey(DEFAULT_SCOPE,
				nodeGen.getNextNodeId(DEFAULT_SCOPE)),
				relationalOp.getRequestedParallelism());
		poPackage.addOriginalLocation(relationalOp.getAlias(),
				relationalOp.getLocation());
		currentPlan.add(poGlobal);
		currentPlan.add(poPackage);

		try {
			currentPlan.connect(poGlobal, poPackage);
		} catch (PlanException e1) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e1);
		}

		int count = 0;
		Byte type = null;
		List<Operator> inputs = relationalOp.getPlan().getPredecessors(
				relationalOp);
		for (int i = 0; i < inputs.size(); i++) {
			Operator op = inputs.get(i);
			List<LogicalExpressionPlan> plans = innerPlans.get(i);
			POLocalRearrange physOp = new POLocalRearrange(new OperatorKey(
					DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)),
					relationalOp.getRequestedParallelism());
			physOp.addOriginalLocation(relationalOp.getAlias(),
					relationalOp.getLocation());
			List<PhysicalPlan> exprPlans = translateExpressionPlans(
					relationalOp, plans);
			try {
				physOp.setPlans(exprPlans);
			} catch (PlanException pe) {
				int errCode = 2071;
				String msg = "Problem with setting up local rearrange's plans.";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, pe);
			}
			try {
				physOp.setIndex(count++);
			} catch (ExecException e1) {
				int errCode = 2058;
				String msg = "Unable to set index on newly create POLocalRearrange.";
				throw new VisitorException(msg, errCode, PigException.BUG, e1);
			}
			if (plans.size() > 1) {
				type = DataType.TUPLE;
				physOp.setKeyType(type);
			} else {
				type = exprPlans.get(0).getLeaves().get(0).getResultType();
				physOp.setKeyType(type);
			}
			physOp.setResultType(DataType.TUPLE);

			currentPlan.add(physOp);

			try {
				currentPlan.connect(logToPhyMap.get(op), physOp);
				currentPlan.connect(physOp, poGlobal);
			} catch (PlanException e) {
				int errCode = 2015;
				String msg = "Invalid physical operators in the physical plan";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, e);
			}
		}

		poPackage.setKeyType(type);
		poPackage.setResultType(DataType.TUPLE);
		poPackage.setNumInps(count);
		poPackage.setInner(innerFlags);
		return poPackage;
	}

	private POForEach compileFE4Flattening(boolean[] innerFlags, String scope,
			int parallel, String alias, SourceLocation location,
			List<Operator> inputs) throws FrontendException {

		List<PhysicalPlan> fePlans = new ArrayList<PhysicalPlan>();
		List<Boolean> flattenLst = new ArrayList<Boolean>();
		POForEach fe;
		try {
			for (int i = 0; i < inputs.size(); i++) {
				PhysicalPlan fep1 = new PhysicalPlan();
				POProject feproj1 = new POProject(new OperatorKey(scope,
						nodeGen.getNextNodeId(scope)), parallel, i + 1); // i+1
				// since
				// the
				// first
				// column
				// is
				// the
				// "group"
				// field
				feproj1.addOriginalLocation(alias, location);
				feproj1.setResultType(DataType.BAG);
				feproj1.setOverloaded(false);
				fep1.add(feproj1);
				fePlans.add(fep1);
				// the parser would have marked the side
				// where we need to keep empty bags on
				// non matched as outer (innerFlags[i] would be
				// false)
				if (!(innerFlags[i])) {
					Operator joinInput = inputs.get(i);
					// for outer join add a bincond
					// which will project nulls when bag is
					// empty
					updateWithEmptyBagCheck(fep1, joinInput);
				}
				flattenLst.add(true);
			}

			fe = new POForEach(new OperatorKey(scope,
					nodeGen.getNextNodeId(scope)), parallel, fePlans,
					flattenLst);
			fe.addOriginalLocation(alias, location);

		} catch (PlanException e1) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e1);
		}
		return fe;
	}

	@Override
	public void visit(LOUnion loUnion) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		POUnion physOp = new POUnion(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)),
				loUnion.getRequestedParallelism());
		physOp.addOriginalLocation(loUnion.getAlias(), loUnion.getLocation());
		currentPlan.add(physOp);
		physOp.setResultType(DataType.BAG);
		physOp.setLogicalSchema(loUnion.getSchema());
		logToPhyMap.put(loUnion, physOp);
		List<Operator> ops = loUnion.getPlan().getPredecessors(loUnion);

		for (Operator l : ops) {
			PhysicalOperator from = logToPhyMap.get(l);
			try {
				currentPlan.connect(from, physOp);
			} catch (PlanException e) {
				int errCode = 2015;
				String msg = "Invalid physical operators in the physical plan";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, e);
			}
		}
	}

	@Override
	public void visit(LOBind loBind) throws FrontendException {
		String scope = DEFAULT_SCOPE;

		POBind poBind = new POBind(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)), loBind.getRequestedParallelism());

		poBind.addOriginalLocation(loBind.getAlias(), loBind.getLocation());

		currentPlan.add(poBind);
		poBind.setResultType(DataType.BAG);
		logToPhyMap.put(loBind, poBind);

		List<Operator> ops = loBind.getPlan().getPredecessors(loBind);

		for (Operator l : ops) {
			PhysicalOperator from = logToPhyMap.get(l);
			try {
				currentPlan.connect(from, poBind);
			} catch (PlanException e) {
				int errCode = 2015;
				String msg = "Invalid physical operators in the physical plan";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.BUG, e);
			}
		}
	}

	@Override
	public void visit(LODistinct loDistinct) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		PODistinct physOp = new PODistinct(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)),
				loDistinct.getRequestedParallelism());
		physOp.setCustomPartitioner(loDistinct.getCustomPartitioner());
		physOp.addOriginalLocation(loDistinct.getAlias(),
				loDistinct.getLocation());
		currentPlan.add(physOp);
		physOp.setResultType(DataType.BAG);
		logToPhyMap.put(loDistinct, physOp);
		Operator op = loDistinct.getPlan().getPredecessors(loDistinct).get(0);

		PhysicalOperator from = logToPhyMap.get(op);
		try {
			currentPlan.connect(from, physOp);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}
	}

	@Override
	public void visit(LOLimit loLimit) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		POLimit poLimit = new POLimit(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)),
				loLimit.getRequestedParallelism());
		poLimit.setLimit(loLimit.getLimit());
		poLimit.addOriginalLocation(loLimit.getAlias(), loLimit.getLocation());
		poLimit.setResultType(DataType.BAG);
		currentPlan.add(poLimit);
		logToPhyMap.put(loLimit, poLimit);

		if (loLimit.getLimitPlan() != null) {
			// add expression plan to POLimit
			currentPlans.push(currentPlan);
			currentPlan = new PhysicalPlan();
			PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(
					loLimit.getLimitPlan());
			pushWalker(childWalker);
			currentWalker
					.walk(new LogicalExpressionCompiler(
							currentWalker.getPlan(), childWalker, loLimit,
							currentPlan, logToPhyMap));
			poLimit.setLimitPlan(currentPlan);
			popWalker();
			currentPlan = currentPlans.pop();
		}

		Operator op = loLimit.getPlan().getPredecessors(loLimit).get(0);

		PhysicalOperator from = logToPhyMap.get(op);
		try {
			currentPlan.connect(from, poLimit);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

		translateSoftLinks(loLimit);
	}

	@Override
	public void visit(LOSplit loSplit) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		POSplit physOp = new POSplit(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)),
				loSplit.getRequestedParallelism());
		physOp.addOriginalLocation(loSplit.getAlias(), loSplit.getLocation());
		physOp.setLogicalSchema(loSplit.getSchema());
		logToPhyMap.put(loSplit, physOp);

		currentPlan.add(physOp);

		List<Operator> op = loSplit.getPlan().getPredecessors(loSplit);
		PhysicalOperator from;

		if (op != null) {
			from = logToPhyMap.get(op.get(0));
		} else {
			int errCode = 2051;
			String msg = "Did not find a predecessor for Split.";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG);
		}

		try {
			currentPlan.connect(from, physOp);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}
	}

	@Override
	public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
		String scope = DEFAULT_SCOPE;
		// System.err.println("Entering Filter");
		POFilter poFilter = new POFilter(new OperatorKey(scope,
				nodeGen.getNextNodeId(scope)),
				loSplitOutput.getRequestedParallelism());
		poFilter.addOriginalLocation(loSplitOutput.getAlias(),
				loSplitOutput.getLocation());
		poFilter.setResultType(DataType.BAG);
		poFilter.setLogicalSchema(loSplitOutput.getSchema());
		currentPlan.add(poFilter);
		logToPhyMap.put(loSplitOutput, poFilter);
		currentPlans.push(currentPlan);

		currentPlan = new PhysicalPlan();

		// PlanWalker childWalker = currentWalker
		// .spawnChildWalker(filter.getFilterPlan());
		PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(
				loSplitOutput.getFilterPlan());
		pushWalker(childWalker);
		// currentWalker.walk(this);
		currentWalker.walk(new LogicalExpressionCompiler(currentWalker
				.getPlan(), childWalker, loSplitOutput, currentPlan,
				logToPhyMap));
		popWalker();

		poFilter.setPlan(currentPlan);
		currentPlan = currentPlans.pop();

		List<Operator> op = loSplitOutput.getPlan().getPredecessors(
				loSplitOutput);

		PhysicalOperator from;
		if (op != null) {
			from = logToPhyMap.get(op.get(0));
		} else {
			int errCode = 2051;
			String msg = "Did not find a predecessor for Filter.";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG);
		}

		try {
			currentPlan.connect(from, poFilter);
		} catch (PlanException e) {
			int errCode = 2015;
			String msg = "Invalid physical operators in the physical plan";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

		translateSoftLinks(loSplitOutput);
		// System.err.println("Exiting Filter");
	}

	/**
	 * updates plan with check for empty bag and if bag is empty to flatten a
	 * bag with as many null's as dictated by the schema
	 * 
	 * @param fePlan
	 *            the plan to update
	 * @param joinInput
	 *            the relation for which the corresponding bag is being checked
	 * @throws FrontendException
	 */
	public static void updateWithEmptyBagCheck(PhysicalPlan fePlan,
			Operator joinInput) throws FrontendException {
		LogicalSchema inputSchema = null;
		try {
			inputSchema = ((LogicalRelationalOperator) joinInput).getSchema();

			if (inputSchema == null) {
				int errCode = 1109;
				String msg = "Input ("
						+ ((LogicalRelationalOperator) joinInput).getAlias()
						+ ") "
						+ "on which outer join is desired should have a valid schema";
				throw new LogicalToPhysicalTranslatorException(msg, errCode,
						PigException.INPUT);
			}
		} catch (FrontendException e) {
			int errCode = 2104;
			String msg = "Error while determining the schema of input";
			throw new LogicalToPhysicalTranslatorException(msg, errCode,
					PigException.BUG, e);
		}

		addEmptyBagOuterJoin(fePlan, Util.translateSchema(inputSchema));

	}

	private void translateSoftLinks(Operator op) throws FrontendException {
		List<Operator> preds = op.getPlan().getSoftLinkPredecessors(op);

		if (preds == null)
			return;

		for (Operator pred : preds) {
			PhysicalOperator from = logToPhyMap.get(pred);
			currentPlan.createSoftLink(from, logToPhyMap.get(op));
		}
	}

	public static void addEmptyBagOuterJoin(PhysicalPlan fePlan,
			Schema inputSchema) throws PlanException {
		// we currently have POProject[bag] as the only operator in the plan
		// If the bag is an empty bag, we should replace
		// it with a bag with one tuple with null fields so that when we flatten
		// we do not drop records (flatten will drop records if the bag is left
		// as an empty bag) and actually project nulls for the fields in
		// the empty bag

		// So we need to get to the following state:
		// POProject[Bag]
		// \
		// POUserFunc["IsEmpty()"] Const[Bag](bag with null fields)
		// \ | POProject[Bag]
		// \ | /
		// POBinCond
		POProject relationProject = (POProject) fePlan.getRoots().get(0);
		try {

			// condition of the bincond
			POProject relationProjectForIsEmpty = relationProject.clone();
			fePlan.add(relationProjectForIsEmpty);
			String scope = relationProject.getOperatorKey().scope;
			FuncSpec isEmptySpec = new FuncSpec(IsEmpty.class.getName());
			Object f = PigContext.instantiateFuncFromSpec(isEmptySpec);
			POUserFunc isEmpty = new POUserFunc(new OperatorKey(scope,
					NodeIdGenerator.getGenerator().getNextNodeId(scope)), -1,
					null, isEmptySpec, (EvalFunc) f);
			isEmpty.setResultType(DataType.BOOLEAN);
			fePlan.add(isEmpty);
			fePlan.connect(relationProjectForIsEmpty, isEmpty);

			// lhs of bincond (const bag with null fields)
			ConstantExpression ce = new ConstantExpression(new OperatorKey(
					scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)));
			// the following should give a tuple with the
			// required number of nulls
			Tuple t = TupleFactory.getInstance().newTuple(inputSchema.size());
			for (int i = 0; i < inputSchema.size(); i++) {
				t.set(i, null);
			}
			List<Tuple> bagContents = new ArrayList<Tuple>(1);
			bagContents.add(t);
			DataBag bg = new NonSpillableDataBag(bagContents);
			ce.setValue(bg);
			ce.setResultType(DataType.BAG);
			// this operator doesn't have any predecessors
			fePlan.add(ce);

			// rhs of bincond is the original project
			// let's set up the bincond now
			POBinCond bincond = new POBinCond(new OperatorKey(scope,
					NodeIdGenerator.getGenerator().getNextNodeId(scope)));
			bincond.setCond(isEmpty);
			bincond.setLhs(ce);
			bincond.setRhs(relationProject);
			bincond.setResultType(DataType.BAG);
			fePlan.add(bincond);

			fePlan.connect(isEmpty, bincond);
			fePlan.connect(ce, bincond);
			fePlan.connect(relationProject, bincond);

		} catch (Exception e) {
			throw new PlanException("Error setting up outerjoin", e);
		}

	}

}
