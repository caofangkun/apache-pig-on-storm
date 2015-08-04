package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.PORelationToExprProject;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POGroup.GroupImp;
import org.apache.storm.executionengine.topologyLayer.Emitter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.data.SchemaTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.TupleMaker;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

//We intentionally skip type checking in backend for performance reasons
@SuppressWarnings("unchecked")
public class POForEach extends PhysicalOperator implements Window {
	private static final Log log = LogFactory.getLog(POForEach.class);
	private static final long serialVersionUID = 1L;
	// private Map<String, List<PhysicalOperator>> windowSeen;
	private boolean windowMode = false;
	protected List<List<PhysicalPlan>> allPlans = new ArrayList<List<PhysicalPlan>>();
	// protected List<ResultContainer> results = new
	// ArrayList<ResultContainer>();
	private GroupImp groupImp = new GroupImp();
	private List<Emitter> emitters = null;
	private transient Timer t = null;
	private int interval = 1;
	private transient Object lock = null;
	private transient Map<Object, GroupResult> cache = null;
	private static final int FLUSH_GROUP_SIZE = 1000 * 24;

	protected List<PhysicalPlan> inputPlans;
	protected List<PhysicalOperator> opsToBeReset;
	// Since the plan has a generate, this needs to be maintained
	// as the generate can potentially return multiple tuples for
	// same call.
	protected boolean processingPlan = false;

	// its holds the iterators of the databags given by the input expressions
	// which need flattening.
	transient protected Iterator<Tuple>[] its = null;

	// This holds the outputs given out by the input expressions of any datatype
	protected Object[] bags = null;

	// This is the template whcih contains tuples and is flattened out in
	// createTuple() to generate the final output
	protected Object[] data = null;

	// store result types of the plan leaves
	protected byte[] resultTypes = null;

	// store whether or not an accumulative UDF has terminated early
	protected BitSet earlyTermination = null;

	// array version of isToBeFlattened - this is purely
	// for optimization - instead of calling isToBeFlattened.get(i)
	// we can do the quicker array access - isToBeFlattenedArray[i].
	// Also we can store "boolean" values rather than "Boolean" objects
	// so we can also save on the Boolean.booleanValue() calls
	protected boolean[] isToBeFlattenedArray;

	ExampleTuple tIn = null;
	protected int noItems;

	protected PhysicalOperator[] planLeafOps = null;

	protected transient AccumulativeTupleBuffer buffer;

	protected Tuple inpTuple;

	private Schema schema;

	public POForEach(OperatorKey k) {
		this(k, -1, null, null);
	}

	public POForEach(OperatorKey k, int rp) {
		this(k, rp, null, null);
	}

	public POForEach(OperatorKey k, List inp) {
		this(k, -1, inp, null);
	}

	public POForEach(OperatorKey k, int rp, List<PhysicalPlan> inp,
			List<Boolean> isToBeFlattened) {
		super(k, rp);
		setUpFlattens(isToBeFlattened);
		this.inputPlans = inp;
		opsToBeReset = new ArrayList<PhysicalOperator>();
		getLeaves();
		// windowSeen = new HashMap<String, List<PhysicalOperator>>();
	}

	public POForEach(OperatorKey operatorKey, int requestedParallelism,
			List<PhysicalPlan> innerPlans, List<Boolean> flattenList,
			Schema schema) {
		this(operatorKey, requestedParallelism, innerPlans, flattenList);
		this.schema = schema;
	}

	private class GroupResult implements Serializable {
		private static final long serialVersionUID = 1L;
		private Tuple lastestAttachTuple = null;
		private List<PhysicalPlan> plans = null;

		public GroupResult() {
			plans = getGeneratePlans();
			prepare();
		}

		public void attachTuple(Tuple t) throws ExecException {
			this.lastestAttachTuple = t;
			Iterator<PhysicalPlan> itp = plans.iterator();
			PhysicalPlan pp = null;
			Iterator<PhysicalOperator> itr = null;
			PhysicalOperator leaf = null;
			while (itp.hasNext()) {
				pp = itp.next();
				itr = pp.getRoots().iterator();
				while (itr.hasNext()) {
					itr.next().attachInput(t);
				}

				leaf = pp.getLeaves().get(0);
				leaf.getNext(leaf.getResultType());
			}
		}

		public Result getFinalResult() throws ExecException {
			if (lastestAttachTuple == null) {
				return null;
			}
			Iterator<PhysicalPlan> itp = plans.iterator();
			PhysicalPlan pp = null;
			Iterator<PhysicalOperator> itr = null;
			while (itp.hasNext()) {
				pp = itp.next();
				itr = pp.getRoots().iterator();
				while (itr.hasNext()) {
					itr.next().attachInput(lastestAttachTuple);
				}
			}
			return getWindowResult();
		}

		private void prepare() {
			for (PhysicalPlan p : plans) {
				Iterator<PhysicalOperator> iter = p.iterator();
				while (iter.hasNext()) {
					PhysicalOperator po = iter.next();
					if (po instanceof ExpressionOperator
							|| po instanceof PODistinct) {
						po.setAccumulative();
						po.setAccumStart();
					} else {
						throw new RuntimeException(
								"Physical operater "
										+ po.getClass()
										+ " can not used in window generate.physcial operator:"
										+ po);
					}
				}
			}
		}

		private List<PhysicalPlan> getGeneratePlans() {
			List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>(
					inputPlans.size());
			PhysicalPlan pl = null;
			try {
				for (PhysicalPlan plan : inputPlans) {
					plans.add(plan.clone());
					pl = plan;
				}
			} catch (CloneNotSupportedException e) {
				log.error(
						"PhysicalPlan list of generete clause can not be cloned.PhyscialPlan:"
								+ pl, e);
				throw new RuntimeException(e);
			}
			return plans;
		}

		private Result getWindowResult() {
			Result r = new Result();
			try {
				accumulateEnd();
				r.result = getWindowResultBag();
				if (r.result != null) {
					r.returnStatus = POStatus.STATUS_OK;
				} else {
					r.returnStatus = POStatus.STATUS_NULL;
				}
			} catch (ExecException e) {
				log.info("Excpetion when get window result.", e);
			} finally {
				accumulateRestart();
			}

			return r;
		}

		private void accumulateEnd() {
			for (PhysicalPlan p : plans) {
				Iterator<PhysicalOperator> iter = p.iterator();
				while (iter.hasNext()) {
					iter.next().setAccumEnd();
				}
			}
		}

		private void accumulateRestart() {
			for (PhysicalPlan p : plans) {
				Iterator<PhysicalOperator> iter = p.iterator();
				while (iter.hasNext()) {
					iter.next().setAccumStart();
				}
			}
		}

		private Tuple getWindowResultBag() throws ExecException {
			if (lastestAttachTuple == null) {
				return null;
			}
			Iterator<PhysicalPlan> itp = this.plans.iterator();
			Tuple t = TupleFactory.getInstance().newTuple(this.plans.size());
			PhysicalOperator leaf = null;
			Result r = null;
			boolean allNullResult = true;
			for (int i = 0; i < plans.size(); i++) {
				leaf = itp.next().getLeaves().get(0);
				try {
					r = leaf.getNext(leaf.getResultType());
				} catch (ExecException e) {
					log.error("Exception when get window result,leaf node:"
							+ leaf + ",attached data:" + lastestAttachTuple);
					throw e;
				}

				if (r.returnStatus == POStatus.STATUS_OK) {
					allNullResult = false;
					t.set(i, r.result);
				} else if (r.returnStatus == POStatus.STATUS_NULL
						|| r.returnStatus == POStatus.STATUS_EOP) {
					t.set(i, null);
				} else {
					log.error("Unexpected result when get window result,result:"
							+ r
							+ ",leaf node:"
							+ leaf
							+ ",attached data:"
							+ lastestAttachTuple);
					return null;
				}
			}

			if (allNullResult) {
				return null;
			}

			return t;
		}
	}

	public void attachInputBag(DataBag... bags) {
		if (windowMode) {
			Result r;
			try {
				r = groupImp.processDataBags(bags);
				DataBag bag = (DataBag) r.result;
				if (bag != null) {
					synchronized (lock) {
						processWindowInput(bag);
						if (cache.size() > FLUSH_GROUP_SIZE) {
							emit(alias, getWindowResult());
						}
					}
				}

			} catch (ExecException e) {
				throw new RuntimeException(
						"Excpetion when attach window input,Window:" + this
								+ ",input data Bag:" + bags[0], e);
			}
		} else {
			super.attachInputBag(bags);
		}
	}

	private void processWindowInput(DataBag bag) throws ExecException {
		Iterator<Tuple> groupIt = bag.iterator();
		Tuple tuple = null;
		Object group = null;
		GroupResult d = null;
		while (groupIt.hasNext()) {
			tuple = groupIt.next();
			group = tuple.get(0);
			d = cache.get(group);
			if (d == null) {
				d = new GroupResult();
				cache.put(group, d);
			}
			d.attachTuple(tuple);
		}
	}

	// public boolean containsConnection(String from) {
	// return windowSeen.containsKey(from);
	// }
	//
	// public void putConnection(String from, List<PhysicalOperator> to) {
	// windowSeen.put(from, to);
	// }
	//
	// public List<PhysicalOperator> getConnection(String from) {
	// return windowSeen.get(from);
	// }
	//
	// public Map<String, List<PhysicalOperator>> getWindowSeen() {
	// return windowSeen;
	// }

	public boolean isWindowMode() {
		return windowMode;
	}

	public void setWindowMode(boolean windowMode) {
		this.windowMode = windowMode;
	}

	public void setInterval(int interval) {
		this.interval = interval;
	}

	public List<List<PhysicalPlan>> getAllPlans() {
		return this.allPlans;
	}

	public void setAllPlans(List<List<PhysicalPlan>> allPlans) {
		this.allPlans = allPlans;
		groupImp.setAllPlans(allPlans);
	}

	@Override
	public void visit(PhyPlanVisitor v) throws VisitorException {
		v.visitPOForEach(this);
	}

	@Override
	public String name() {
		if (isWindowMode()) {
			return getAliasString() + "New Window" + "(" + getFlatStr() + ")"
					+ "[" + DataType.findTypeName(resultType) + "]" + " - "
					+ mKey.toString();
		} else {
			return getAliasString() + "New For Each" + "(" + getFlatStr() + ")"
					+ "[" + DataType.findTypeName(resultType) + "]" + " - "
					+ mKey.toString();
		}
	}

	String getFlatStr() {
		if (isToBeFlattenedArray == null) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (Boolean b : isToBeFlattenedArray) {
			sb.append(b);
			sb.append(',');
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}

	@Override
	public Result processDataBags(DataBag... bags) throws ExecException {
		if (windowMode) {
			Result r = groupImp.processDataBags(bags);
			DataBag bag = (DataBag) r.result;
			DataBag[] groupedBags = new DataBag[1];
			groupedBags[0] = bag;
			return this.processForEach(groupedBags);
		} else {
			return this.processForEach(bags);
		}
	}

	@Override
	public boolean supportsMultipleInputs() {
		return false;
	}

	@Override
	public boolean supportsMultipleOutputs() {
		return true;
	}

	public Schema getSchema() {
		return this.schema;
	}

	public Result processForEach(DataBag... bags) throws ExecException {
		DataBag bag = bags[0];
		Result r = new Result();
		if (bag == null) {
			r.returnStatus = POStatus.STATUS_NULL;
			return r;
		}
		// TODO:2.ForEach
		Iterator<Tuple> it = bag.iterator();
		List<Tuple> tuples = new ArrayList<Tuple>();

		try {
			Result res = null;
			Tuple inp = null;

			// The nested plan processing is done or is
			// yet to begin. So process the input and start
			// nested plan processing on the input tuple
			// read
			while (it.hasNext()) {
				inp = it.next();
				attachInputToPlans(inp);
				inpTuple = inp;
				for (PhysicalOperator po : opsToBeReset) {
					po.reset();
				}
				res = processPlan();
				processingPlan = true;
				tuples.add((Tuple) res.result);
				// TODO:Nested Block
				for (PhysicalPlan plan : inputPlans) {
					plan.detachAllInputBag();
				}

				// The nested plan is under processing
				// So return tuples that the generate oper
				// returns
				while (processingPlan) {
					res = processPlan();
					if (res.returnStatus == POStatus.STATUS_OK) {
						tuples.add((Tuple) res.result);
						log.debug("Foreach output:" + alias + ":"
								+ (Tuple) res.result);
					}
					if (res.returnStatus == POStatus.STATUS_EOP) {
						processingPlan = false;
						for (PhysicalPlan plan : inputPlans) {
							plan.detachInput();
							plan.detachAllInputBag();
						}
						break;
					}
					if (res.returnStatus == POStatus.STATUS_ERR) {
						tuples.add((Tuple) res.result);
					}
					if (res.returnStatus == POStatus.STATUS_NULL) {
						continue;
					}
				}
			}
		} catch (RuntimeException e) {
			throw new ExecException("Error while executing ForEach at "
					+ this.getOriginalLocations(), e);
		}
		//
		if (tuples.size() == 0) {
			r.returnStatus = POStatus.STATUS_NULL;
			r.result = null;
		} else {
			r.returnStatus = POStatus.STATUS_OK;
			r.result = new NonSpillableDataBag(tuples);
		}

		return r;
	}

	/**
	 * Calls getNext on the generate operator inside the nested physical plan
	 * and returns it maintaining an additional state to denote the begin and
	 * end of the nested plan processing.
	 */
	@Override
	public Result getNextTuple() throws ExecException {
		try {
			Result res = null;
			Result inp = null;
			// The nested plan is under processing
			// So return tuples that the generate oper
			// returns
			if (processingPlan) {
				while (true) {
					res = processPlan();

					if (res.returnStatus == POStatus.STATUS_OK) {
						return res;
					}
					if (res.returnStatus == POStatus.STATUS_EOP) {
						processingPlan = false;
						for (PhysicalPlan plan : inputPlans) {
							plan.detachInput();
						}
						break;
					}
					if (res.returnStatus == POStatus.STATUS_ERR) {
						return res;
					}
					if (res.returnStatus == POStatus.STATUS_NULL) {
						continue;
					}
				}
			}
			// The nested plan processing is done or is
			// yet to begin. So process the input and start
			// nested plan processing on the input tuple
			// read
			while (true) {
				inp = processInput();
				if (inp.returnStatus == POStatus.STATUS_EOP
						|| inp.returnStatus == POStatus.STATUS_ERR) {
					return inp;
				}
				if (inp.returnStatus == POStatus.STATUS_NULL) {
					continue;
				}

				attachInputToPlans((Tuple) inp.result);
				inpTuple = (Tuple) inp.result;

				for (PhysicalOperator po : opsToBeReset) {
					po.reset();
				}

				res = processPlan();

				processingPlan = true;

				return res;
			}
		} catch (RuntimeException e) {
			throw new ExecException("Error while executing ForEach at "
					+ this.getOriginalLocations(), e);
		}
	}

	private TupleMaker<? extends Tuple> tupleMaker;
	private boolean knownSize = false;

	protected Result processPlan() throws ExecException {
		if (schema != null && tupleMaker == null) {
			// Note here that if SchemaTuple is currently turned on, then any
			// UDF's in the chain
			// must follow good practices. Namely, they should not append to the
			// Tuple that comes
			// out of an iterator (a practice which is fairly common, but is not
			// recommended).

			SchemaTupleBackend.initialize(true);

			tupleMaker = SchemaTupleFactory.getInstance(schema, false,
					GenContext.FOREACH);
			if (tupleMaker != null) {
				knownSize = true;
			}

		}
		if (tupleMaker == null) {
			tupleMaker = TupleFactory.getInstance();
		}

		Result res = new Result();

		// We check if all the databags have exhausted the tuples. If so we
		// enforce the reading of new data by setting data and its to null
		if (its != null) {
			boolean restartIts = true;
			for (int i = 0; i < noItems; ++i) {
				if (its[i] != null && isToBeFlattenedArray[i] == true) {
					restartIts &= !its[i].hasNext();
				}
			}
			// this means that all the databags have reached their last
			// elements. so we need to force reading of fresh databags
			if (restartIts) {
				its = null;
				data = null;
			}
		}

		if (its == null) {
			// getNext being called for the first time OR starting with a set of
			// new data from inputs
			its = new Iterator[noItems];
			bags = new Object[noItems];
			earlyTermination = new BitSet(noItems);

			for (int i = 0; i < noItems; ++i) {
				// Getting the iterators
				// populate the input data
				Result inputData = null;
				switch (resultTypes[i]) {
				case DataType.BAG:
				case DataType.TUPLE:
				case DataType.BYTEARRAY:
				case DataType.MAP:
				case DataType.BOOLEAN:
				case DataType.INTEGER:
				case DataType.DOUBLE:
				case DataType.LONG:
				case DataType.FLOAT:
				case DataType.BIGINTEGER:
				case DataType.BIGDECIMAL:
				case DataType.DATETIME:
				case DataType.CHARARRAY:
					inputData = planLeafOps[i].getNext(resultTypes[i]);
					break;
				default: {
					int errCode = 2080;
					String msg = "Foreach currently does not handle type "
							+ DataType.findTypeName(resultTypes[i]);
					throw new ExecException(msg, errCode, PigException.BUG);
				}

				}

				// we accrue information about what accumulators have early
				// terminated
				// in the case that they all do, we can finish
				if (inputData.returnStatus == POStatus.STATUS_EARLY_TERMINATION) {
					if (!earlyTermination.get(i))
						earlyTermination.set(i);

					continue;
				}

				if (inputData.returnStatus == POStatus.STATUS_BATCH_OK) {
					continue;
				}

				if (inputData.returnStatus == POStatus.STATUS_EOP) {
					// we are done with all the elements. Time to return.
					its = null;
					bags = null;
					return inputData;
				}
				// if we see a error just return it
				if (inputData.returnStatus == POStatus.STATUS_ERR) {
					return inputData;
				}

				// Object input = null;

				bags[i] = inputData.result;

				if (inputData.result instanceof DataBag
						&& isToBeFlattenedArray[i]) {
					its[i] = ((DataBag) bags[i]).iterator();
				} else {
					its[i] = null;
				}
			}
		}

		// if accumulating, we haven't got data yet for some fields, just return
		if (isAccumulative() && isAccumStarted()) {
			if (earlyTermination.cardinality() < noItems) {
				res.returnStatus = POStatus.STATUS_BATCH_OK;
			} else {
				res.returnStatus = POStatus.STATUS_EARLY_TERMINATION;
			}
			return res;
		}

		while (true) {
			if (data == null) {
				// getNext being called for the first time or starting on new
				// input data
				// we instantiate the template array and start populating it
				// with data
				data = new Object[noItems];
				for (int i = 0; i < noItems; ++i) {
					if (isToBeFlattenedArray[i] && bags[i] instanceof DataBag) {
						if (its[i].hasNext()) {
							data[i] = its[i].next();
						} else {
							// the input set is null, so we return with EOP.
							// This is
							// caught above and this function recalled with
							// new inputs.
							its = null;
							data = null;
							res.returnStatus = POStatus.STATUS_EOP;
							return res;
						}
					} else {
						data[i] = bags[i];
					}

				}
				if (getReporter() != null) {
					getReporter().progress();
				}
				// createTuple(data);
				res.result = createTuple(data);
				res.returnStatus = POStatus.STATUS_OK;
				return res;
			} else {
				// we try to find the last expression which needs flattening and
				// start iterating over it
				// we also try to update the template array
				for (int index = noItems - 1; index >= 0; --index) {
					if (its[index] != null && isToBeFlattenedArray[index]) {
						if (its[index].hasNext()) {
							data[index] = its[index].next();
							res.result = createTuple(data);
							res.returnStatus = POStatus.STATUS_OK;
							return res;
						} else {
							// reset this index's iterator so cross product can
							// be achieved
							// we would be resetting this way only for the
							// indexes from the end
							// when the first index which needs to be flattened
							// has reached the
							// last element in its iterator, we won't come here
							// - instead, we reset
							// all iterators at the beginning of this method.
							its[index] = ((DataBag) bags[index]).iterator();
							data[index] = its[index].next();
						}
					}
				}
			}
		}

		// return null;
	}

	/**
	 * 
	 * @param data
	 *            array that is the template for the final flattened tuple
	 * @return the final flattened tuple
	 */
	protected Tuple createTuple(Object[] data) throws ExecException {
		Tuple out = tupleMaker.newTuple();

		int idx = 0;
		for (int i = 0; i < data.length; ++i) {
			Object in = data[i];

			if (isToBeFlattenedArray[i] && in instanceof Tuple) {
				Tuple t = (Tuple) in;
				int size = t.size();
				for (int j = 0; j < size; ++j) {
					if (knownSize) {
						out.set(idx++, t.get(j));
					} else {
						out.append(t.get(j));
					}
				}
			} else {
				if (knownSize) {
					out.set(idx++, in);
				} else {
					out.append(in);
				}
			}
		}
		if (inpTuple != null) {
			return illustratorMarkup(inpTuple, out, 0);
		} else {
			return illustratorMarkup2(data, out);
		}
	}

	protected void attachInputToPlans(Tuple t) {
		for (PhysicalPlan p : inputPlans) {
			p.attachInput(t);
		}
	}

	public void getLeaves() {
		if (inputPlans != null) {
			int i = -1;
			if (isToBeFlattenedArray == null) {
				isToBeFlattenedArray = new boolean[inputPlans.size()];
			}
			planLeafOps = new PhysicalOperator[inputPlans.size()];
			for (PhysicalPlan p : inputPlans) {
				++i;
				PhysicalOperator leaf = p.getLeaves().get(0);
				planLeafOps[i] = leaf;
				if (leaf instanceof POProject
						&& leaf.getResultType() == DataType.TUPLE
						&& ((POProject) leaf).isProjectToEnd()) {
					isToBeFlattenedArray[i] = true;
				}
			}
		}
		// we are calculating plan leaves
		// so lets reinitialize
		reInitialize();
	}

	private void reInitialize() {
		if (planLeafOps != null) {
			noItems = planLeafOps.length;
			resultTypes = new byte[noItems];
			for (int i = 0; i < resultTypes.length; i++) {
				resultTypes[i] = planLeafOps[i].getResultType();
			}
		} else {
			noItems = 0;
			resultTypes = null;
		}

		if (inputPlans != null) {
			for (PhysicalPlan pp : inputPlans) {
				try {
					ResetFinder lf = new ResetFinder(pp, opsToBeReset);
					lf.visit();
				} catch (VisitorException ve) {
					String errMsg = "Internal Error:  Unexpected error looking for nested operators which need to be reset in FOREACH";
					throw new RuntimeException(errMsg, ve);
				}
			}
		}
	}

	public List<PhysicalPlan> getInputPlans() {
		return inputPlans;
	}

	public void setInputPlans(List<PhysicalPlan> plans) {
		inputPlans = plans;
		planLeafOps = null;
		getLeaves();
	}

	public void addInputPlan(PhysicalPlan plan, boolean flatten) {
		inputPlans.add(plan);
		// add to planLeafOps
		// copy existing leaves
		PhysicalOperator[] newPlanLeafOps = new PhysicalOperator[planLeafOps.length + 1];
		for (int i = 0; i < planLeafOps.length; i++) {
			newPlanLeafOps[i] = planLeafOps[i];
		}
		// add to the end
		newPlanLeafOps[planLeafOps.length] = plan.getLeaves().get(0);
		planLeafOps = newPlanLeafOps;

		// add to isToBeFlattenedArray
		// copy existing values
		boolean[] newIsToBeFlattenedArray = new boolean[isToBeFlattenedArray.length + 1];
		for (int i = 0; i < isToBeFlattenedArray.length; i++) {
			newIsToBeFlattenedArray[i] = isToBeFlattenedArray[i];
		}
		// add to end
		newIsToBeFlattenedArray[isToBeFlattenedArray.length] = flatten;
		isToBeFlattenedArray = newIsToBeFlattenedArray;

		// we just added a leaf - reinitialize
		reInitialize();
	}

	public void setToBeFlattened(List<Boolean> flattens) {
		setUpFlattens(flattens);
	}

	public List<Boolean> getToBeFlattened() {
		List<Boolean> result = null;
		if (isToBeFlattenedArray != null) {
			result = new ArrayList<Boolean>();
			for (int i = 0; i < isToBeFlattenedArray.length; i++) {
				result.add(isToBeFlattenedArray[i]);
			}
		}
		return result;
	}

	/**
	 * Make a deep copy of this operator.
	 * 
	 * @throws CloneNotSupportedException
	 */
	@Override
	public POForEach clone() throws CloneNotSupportedException {
		List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>(
				inputPlans.size());
		for (PhysicalPlan plan : inputPlans) {
			plans.add(plan.clone());
		}
		List<Boolean> flattens = null;
		if (isToBeFlattenedArray != null) {
			flattens = new ArrayList<Boolean>(isToBeFlattenedArray.length);
			for (boolean b : isToBeFlattenedArray) {
				flattens.add(b);
			}
		}

		List<PhysicalOperator> ops = new ArrayList<PhysicalOperator>(
				opsToBeReset.size());
		for (PhysicalOperator op : opsToBeReset) {
			ops.add(op);
		}
		POForEach clone = new POForEach(new OperatorKey(mKey.scope,
				NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)),
				requestedParallelism, plans, flattens);
		clone.setOpsToBeReset(ops);
		clone.setResultType(getResultType());
		clone.addOriginalLocation(alias, getOriginalLocations());
		return clone;
	}

	public boolean inProcessing() {
		return processingPlan;
	}

	protected void setUpFlattens(List<Boolean> isToBeFlattened) {
		if (isToBeFlattened == null) {
			isToBeFlattenedArray = null;
		} else {
			isToBeFlattenedArray = new boolean[isToBeFlattened.size()];
			int i = 0;
			for (Iterator<Boolean> it = isToBeFlattened.iterator(); it
					.hasNext();) {
				isToBeFlattenedArray[i++] = it.next();
			}
		}
	}

	/**
	 * Visits a pipeline and calls reset on all the nodes. Currently only pays
	 * attention to limit nodes, each of which need to be told to reset their
	 * limit.
	 */
	private class ResetFinder extends PhyPlanVisitor {

		ResetFinder(PhysicalPlan plan, List<PhysicalOperator> toBeReset) {
			super(plan,
					new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(
							plan));
		}

		@Override
		public void visitDistinct(PODistinct d) throws VisitorException {
			// FIXME: add only if limit is present
			opsToBeReset.add(d);
		}

		@Override
		public void visitLimit(POLimit limit) throws VisitorException {
			opsToBeReset.add(limit);
		}

		@Override
		public void visitSort(POSort sort) throws VisitorException {
			// FIXME: add only if limit is present
			opsToBeReset.add(sort);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans
		 * .PhyPlanVisitor
		 * #visitProject(org.apache.pig.backend.hadoop.executionengine
		 * .physicalLayer.expressionOperators.POProject)
		 */
		@Override
		public void visitProject(POProject proj) throws VisitorException {
			if (proj instanceof PORelationToExprProject) {
				opsToBeReset.add(proj);
			}
		}
	}

	/**
	 * @return the opsToBeReset
	 */
	public List<PhysicalOperator> getOpsToBeReset() {
		return opsToBeReset;
	}

	/**
	 * @param opsToBeReset
	 *            the opsToBeReset to set
	 */
	public void setOpsToBeReset(List<PhysicalOperator> opsToBeReset) {
		this.opsToBeReset = opsToBeReset;
	}

	private Tuple illustratorMarkup2(Object[] in, Object out) {
		if (illustrator != null) {
			ExampleTuple tOut = new ExampleTuple((Tuple) out);
			illustrator.getLineage().insert(tOut);
			boolean synthetic = false;
			for (Object tIn : in) {
				synthetic |= ((ExampleTuple) tIn).synthetic;
				illustrator.getLineage().union(tOut, (Tuple) tIn);
			}
			illustrator.addData(tOut);
			int i;
			for (i = 0; i < noItems; ++i) {
				if (((DataBag) bags[i]).size() < 2) {
					break;
				}
			}
			if (i >= noItems && !illustrator.getEqClassesShared()) {
				illustrator.getEquivalenceClasses().get(0).add(tOut);
			}
			tOut.synthetic = synthetic;
			return tOut;
		} else {
			return (Tuple) out;
		}
	}

	@Override
	public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
		if (illustrator != null) {
			ExampleTuple tOut = new ExampleTuple((Tuple) out);
			illustrator.addData(tOut);
			if (!illustrator.getEqClassesShared()) {
				illustrator.getEquivalenceClasses().get(0).add(tOut);
			}
			LineageTracer lineageTracer = illustrator.getLineage();
			lineageTracer.insert(tOut);
			tOut.synthetic = ((ExampleTuple) in).synthetic;
			lineageTracer.union((ExampleTuple) in, tOut);
			return tOut;
		} else {
			return (Tuple) out;
		}
	}

	@Override
	public boolean isWindowOperator() {
		return isWindowMode();
	}

	@Override
	public void addEmitter(Emitter emitter) {
		if (this.emitters == null) {
			emitters = new ArrayList<Emitter>();
		}
		this.emitters.add(emitter);
	}

	private Result getWindowResult() {
		Result r = new Result();
		synchronized (lock) {
			try {
				if (cache.size() == 0) {
					r.returnStatus = POStatus.STATUS_NULL;
					return r;
				}

				Iterator<Object> it = cache.keySet().iterator();
				Object group = null;
				GroupResult d = null;
				Result groupResult = null;
				DataBag resultBag = new NonSpillableDataBag(cache.size());
				while (it.hasNext()) {
					group = it.next();
					d = cache.get(group);
					groupResult = d.getFinalResult();
					if (groupResult.returnStatus == POStatus.STATUS_OK) {
						resultBag.add((Tuple) groupResult.result);
					} else {
						log.error("Window group don't have result,Group:"
								+ group + ",Window:" + POForEach.this);
					}
				}

				if (resultBag.size() > 0) {
					r.result = resultBag;
					r.returnStatus = POStatus.STATUS_OK;
				} else {
					r.result = null;
					r.returnStatus = POStatus.STATUS_NULL;
				}
			} catch (ExecException e) {
				log.info("Excpetion when get window result.", e);
			} finally {
				cache.clear();
			}
		}
		return r;
	}

	@Override
	public void prepare(Map stormConf, ExecutorService executor,
			final String streamId) {
		lock = new Object();
		cache = new HashMap<Object, GroupResult>();

		final long period = this.interval * 1000;
		t = new Timer(name());
		TimerTask tt = new TimerTask() {
			public void run() {
				Result ret = null;
				try {
					ret = getWindowResult();
				} catch (Exception e) {
					log.error("Exception when get window result,POForeach:"
							+ POForEach.this, e);
					return;
				}
				if (ret.returnStatus == POStatus.STATUS_OK) {
					emit(streamId, ret);
				}
			}
		};
		t.schedule(tt, period, period);
	}

	private void emit(String alias, Result ret) {
		if (ret.returnStatus == POStatus.STATUS_NULL) {
			return;
		} else if (ret.returnStatus != POStatus.STATUS_OK) {
			log.error("Result not OK or NULL,Result:" + ret);
			return;
		}

		try {
			for (Emitter emitter : emitters) {
				emitter.emit(POForEach.this.alias, (DataBag) ret.result);
				log.debug("Window output:" + POForEach.this.alias + ":"
						+ (DataBag) ret.result);
			}
		} catch (Exception e) {
			log.error("Exception when emit data.Current alias:"
					+ POForEach.this.alias + ",databag:" + ret.result, e);
		}
	}

	@Override
	public void cleanup() {
		t.cancel();
		emit(alias, getWindowResult());
	}
}
