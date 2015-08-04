package org.apache.storm.executionengine.physicalLayer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POBind;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POGroup;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.parser.SourceLocation;
import org.apache.pig.pen.Illustrable;
import org.apache.pig.pen.Illustrator;

/**
 * 
 * This is the base class for all operators. This supports a generic way of
 * processing inputs which can be overridden by operators extending this class.
 * The input model assumes that it can either be taken from an operator or can
 * be attached directly to this operator. Also it is assumed that inputs to an
 * operator are always in the form of a tuple.
 * 
 * For this pipeline rework, we assume a pull based model, i.e, the root
 * operator is going to call getNext with the appropriate type which initiates a
 * cascade of getNext calls that unroll to create input for the root operator to
 * work on.
 * 
 * Any operator that extends the PhysicalOperator, supports a getNext with all
 * the different types of parameter types. The concrete implementation should
 * use the result type of its input operator to decide the type of getNext's
 * parameter. This is done to avoid switch/case based on the type as much as
 * possible. The default is assumed to return an erroneus Result corresponding
 * to an unsupported operation on that type. So the operators need to implement
 * only those types that are supported.
 * 
 */
public abstract class PhysicalOperator extends Operator<PhyPlanVisitor>
		implements Illustrable, Cloneable {

	private static final Log log = LogFactory.getLog(PhysicalOperator.class);

	protected static final long serialVersionUID = 1L;

	// The degree of parallelism requested
	protected int requestedParallelism;

	// The inputs that this operator will read data from,now not
	// supportsMultipleInputs.
	protected List<PhysicalOperator> inputs;

	// The outputs that this operator will write data to
	// Will be used to create Targeted tuples,now supportsMultipleOutputs.
	protected List<PhysicalOperator> outputs;

	// The data type for the results of this operator
	protected byte resultType = DataType.TUPLE;

	// The physical plan this operator is part of
	protected PhysicalPlan parentPlan;

	// Specifies if the input has been directly attached
	protected boolean inputAttached = false;

	protected Tuple input = null;

	// Specifies if the input has been directly attached
	protected boolean inputBagsAttached = false;

	protected DataBag[] inputBags = null;

	protected Result lastResult = null;

	// The result of performing the operation along with the output
	protected Result res = null;

	// alias associated with this PhysicalOperator
	protected String alias = null;

	// Will be used by operators to report status or transmit heartbeat
	// Should be set by the backends to appropriate implementations that
	// wrap their own version of a reporter.
	private static ThreadLocal<PigProgressable> reporter = new ThreadLocal<PigProgressable>();

	// Will be used by operators to aggregate warning messages
	// Should be set by the backends to appropriate implementations that
	// wrap their own version of a logger.
	protected static PigLogger pigLogger;

	protected transient Illustrator illustrator = null;

	private boolean accum;

	private transient boolean accumStart;

	private List<OriginalLocation> originalLocations = new ArrayList<OriginalLocation>();

	private Map<String, Integer> inputAliasToIndex = null;

	// schema for the output
	protected LogicalSchema logicalSchema;

	public PhysicalOperator(OperatorKey k) {
		this(k, -1, null);
	}

	public PhysicalOperator(OperatorKey k, int rp) {
		this(k, rp, null);
	}

	public PhysicalOperator(OperatorKey k, List<PhysicalOperator> inp) {
		this(k, -1, inp);
	}

	public PhysicalOperator(OperatorKey k, int rp, List<PhysicalOperator> inp) {
		super(k);
		requestedParallelism = rp;
		inputs = inp;
		res = new Result();
	}

	/**
	 * TODO:Get the schema for the output of this operator. This does not merely
	 * return the schema variable. If schema is not yet set, this will attempt
	 * to construct it.
	 * 
	 * @return the schema
	 */
	public Schema getSchema() {
		if (logicalSchema != null) {
			Schema s = org.apache.pig.newplan.logical.Util
					.translateSchema(logicalSchema);
			return s;
		}
		return null;
	}

	public void setLogicalSchema(LogicalSchema logicalSchema) {
		this.logicalSchema = logicalSchema;
	}

	@Override
	public void setIllustrator(Illustrator illustrator) {
		this.illustrator = illustrator;
	}

	public Illustrator getIllustrator() {
		return illustrator;
	}

	public int getRequestedParallelism() {
		return requestedParallelism;
	}

	public void setRequestedParallelism(int requestedParallelism) {
		this.requestedParallelism = requestedParallelism;
	}

	public byte getResultType() {
		return resultType;
	}

	public String getAlias() {
		return alias;
	}

	protected String getAliasString() {
		return (alias == null) ? "" : (alias + ": ");
	}

	public void addOriginalLocation(String alias, SourceLocation sourceLocation) {
		this.alias = alias;
		this.originalLocations.add(new OriginalLocation(alias, sourceLocation
				.line(), sourceLocation.offset()));
	}

	public void addOriginalLocation(String alias,
			List<OriginalLocation> originalLocations) {
		this.alias = alias;
		this.originalLocations.addAll(originalLocations);
	}

	public List<OriginalLocation> getOriginalLocations() {
		return Collections.unmodifiableList(originalLocations);
	}

	public boolean supportsMultipleOutputs() {
		return true;
	}

	public void setAccumulative() {
		accum = true;
	}

	public boolean isAccumulative() {
		return accum;
	}

	public void setAccumStart() {
		if (!accum) {
			throw new IllegalStateException("Accumulative is not turned on.");
		}
		accumStart = true;
	}

	public boolean isAccumStarted() {
		return accumStart;
	}

	public void setAccumEnd() {
		if (!accum) {
			throw new IllegalStateException("Accumulative is not turned on.");
		}
		accumStart = false;
	}

	public void setResultType(byte resultType) {
		this.resultType = resultType;
	}

	public String[] getInputAlias() {
		Set<String> alias = new HashSet<String>();
		if (inputs != null && inputs.size() > 0) {
			for (PhysicalOperator po : inputs) {
				if (po instanceof POBind) {
					alias.addAll(((POBind) po).getPartitionSeen().keySet());
				} else {
					alias.add(po.getAlias());
				}
			}
		}
		return alias.toArray(new String[alias.size()]);
	}

	public List<PhysicalOperator> getInputs() {
		return inputs;
	}

	public void setInputs(List<PhysicalOperator> inputs) {
		this.inputs = inputs;
		this.inputAliasToIndex = null;
	}

	public boolean isInputAttached() {
		return inputAttached;
	}

	/**
	 * Shorts the input path of this operator by providing the input tuple
	 * directly
	 * 
	 * @param t
	 *            - The tuple that should be used as input
	 */
	public void attachInput(Tuple b) {
		input = b;
		this.inputAttached = true;
	}

	/**
	 * Detaches any tuples that are attached
	 * 
	 */
	public void detachInput() {
		input = null;
		this.inputAttached = false;
	}

	public void attachInputBag(String alias, DataBag bag) {
		String[] inputAlias = this.getInputAlias();
		if (this.inputAliasToIndex == null) {
			this.inputAliasToIndex = new HashMap<String, Integer>(
					inputAlias.length);

			for (int i = 0; i < inputAlias.length; i++) {
				this.inputAliasToIndex.put(inputAlias[i], i);
			}
		}

		int opIndex = this.inputAliasToIndex.get(alias);
		DataBag[] bags = new DataBag[inputAlias.length];
		bags[opIndex] = bag;
		if (this instanceof POGroup || this instanceof POUnion) {
			this.attachInputBag(bags);
		} else {
			this.attachInputBag(bag);
		}
	}

	public void attachInputBag(DataBag... bag) {
		this.inputBags = bag;
		this.inputBagsAttached = true;
		this.lastResult = null;
	}

	public void detachInputBag() {
		this.inputBags = null;
		this.inputBagsAttached = false;
		this.lastResult = null;
	}

	// TODO should be more efficient
	public void recursiveDetachInput() {
		this.detachInput();
		if (this.inputs != null && this.inputs.size() > 0) {
			for (PhysicalOperator input : this.inputs) {
				input.recursiveDetachInput();
			}
		}
	}

	public void recursiveDetachInputBag() {
		this.detachInputBag();
		if (this.inputs != null && this.inputs.size() > 0) {
			for (PhysicalOperator input : this.inputs) {
				input.recursiveDetachInputBag();
			}
		}
	}

	/**
	 * A blocking operator should override this to return true. Blocking
	 * operators are those that need the full bag before operate on the tuples
	 * inside the bag. Example is the Global Rearrange. Non-blocking or pipeline
	 * operators are those that work on a tuple by tuple basis.
	 * 
	 * @return true if blocking and false otherwise
	 */
	public boolean isBlocking() {
		return false;
	}

	/**
	 * A generic method for parsing input that either returns the attached input
	 * if it exists or fetches it from its predecessor. If special processing is
	 * required, this method should be overridden.
	 * 
	 * @return The Result object that results from processing the input
	 * @throws ExecException
	 */
	public Result processInput() throws ExecException {
		try {
			Result res = new Result();
			if (input == null && (inputs == null || inputs.size() == 0)) {
				// log.warn("No inputs found. Signaling End of Processing.");
				res.returnStatus = POStatus.STATUS_EOP;
				return res;
			}

			// Should be removed once the model is clear
			if (getReporter() != null) {
				getReporter().progress();
			}

			if (!isInputAttached()) {
				return inputs.get(0).getNextTuple();
			} else {
				res.result = input;
				res.returnStatus = (res.result == null ? POStatus.STATUS_NULL
						: POStatus.STATUS_OK);
				detachInput();
				return res;
			}
		} catch (ExecException e) {
			throw new ExecException("Exception while executing "
					+ this.toString() + ": " + e.toString(), e);
		}
	}

	@Override
	public abstract void visit(PhyPlanVisitor v) throws VisitorException;

	/**
	 * Implementations that call into the different versions of getNext are
	 * often identical, differing only in the signature of the getNext() call
	 * they make. This method allows to cut down on some of the copy-and-paste.
	 * 
	 * @param dataType
	 *            Describes the type of obj; a byte from DataType.
	 * 
	 * @return result Result of applying this Operator to the Object.
	 * @throws ExecException
	 */
	public Result getNext(byte dataType) throws ExecException {
		try {
			switch (dataType) {
			case DataType.BAG:
				return getNextDataBag();
			case DataType.BOOLEAN:
				return getNextBoolean();
			case DataType.BYTEARRAY:
				return getNextDataByteArray();
			case DataType.CHARARRAY:
				return getNextString();
			case DataType.DOUBLE:
				return getNextDouble();
			case DataType.FLOAT:
				return getNextFloat();
			case DataType.INTEGER:
				return getNextInteger();
			case DataType.LONG:
				return getNextLong();
			case DataType.BIGINTEGER:
				return getNextBigInteger();
			case DataType.BIGDECIMAL:
				return getNextBigDecimal();
			case DataType.DATETIME:
				return getNextDateTime();
			case DataType.MAP:
				return getNextMap();
			case DataType.TUPLE:
				return getNextTuple();
			default:
				throw new ExecException("Unsupported type for getNext: "
						+ DataType.findTypeName(dataType));
			}
		} catch (RuntimeException e) {
			throw new ExecException("Exception while executing "
					+ this.toString() + ": " + e.toString(), e);
		}
	}

	public Result getNextInteger() throws ExecException {
		return res;
	}

	public Result getNextLong() throws ExecException {
		return res;
	}

	public Result getNextDouble() throws ExecException {
		return res;
	}

	public Result getNextFloat() throws ExecException {
		return res;
	}

	public Result getNextDateTime() throws ExecException {
		return res;
	}

	public Result getNextString() throws ExecException {
		return res;
	}

	public Result getNextDataByteArray() throws ExecException {
		return res;
	}

	public Result getNextMap() throws ExecException {
		return res;
	}

	public Result getNextBoolean() throws ExecException {
		return res;
	}

	public Result getNextTuple() throws ExecException {
		return res;
	}

	public Result processDataBags(DataBag... bags) throws ExecException {
		return null;
	}

	public Result getNextDataBag() throws ExecException {
		if (this.inputBagsAttached) {
			if (this.lastResult != null) {
				return this.lastResult;
			}

			Result r = new Result();
			if (this.inputBags == null) {
				r.result = null;
				r.returnStatus = POStatus.STATUS_NULL;
				return r;
			}

			r = processDataBags(this.inputBags);
			this.lastResult = r;

			return r;
		} else {
			if (this.inputs.size() == 0) {
				Result r = new Result();
				r.returnStatus = POStatus.STATUS_ERR;
				return r;
			}

			DataBag[] opers = new DataBag[this.inputs.size()];
			boolean allInputNull = true;
			for (int i = 0; i < opers.length; i++) {
				Result r = this.inputs.get(i).getNextDataBag();
				if (r.returnStatus != POStatus.STATUS_OK
						&& r.returnStatus != POStatus.STATUS_NULL) {
					r.result = null;
					return r;
				}
				opers[i] = (DataBag) r.result;
				if (r.returnStatus != POStatus.STATUS_NULL) {
					allInputNull = false;
				}
			}

			this.attachInputBag(allInputNull ? null : opers);
			return getNextDataBag();
		}
	}

	public Result getNextBigInteger() throws ExecException {
		return res;
	}

	public Result getNextBigDecimal() throws ExecException {
		return res;
	}

	/**
	 * Reset internal state in an operator. For use in nested pipelines where
	 * operators like limit and sort may need to reset their state. Limit needs
	 * it because it needs to know it's seeing a fresh set of input. Blocking
	 * operators like sort and distinct need it because they may not have
	 * drained their previous input due to a limit and thus need to be told to
	 * drop their old input and start over.
	 */
	public void reset() {
	}

	/**
	 * @return PigProgressable stored in threadlocal
	 */
	public static PigProgressable getReporter() {
		return PhysicalOperator.reporter.get();
	}

	/**
	 * @param reporter
	 *            PigProgressable to be stored in threadlocal
	 */
	public static void setReporter(PigProgressable reporter) {
		PhysicalOperator.reporter.set(reporter);
	}

	/**
	 * Make a deep copy of this operator. This function is blank, however, we
	 * should leave a place holder so that the subclasses can clone
	 * 
	 * @throws CloneNotSupportedException
	 */
	@Override
	public PhysicalOperator clone() throws CloneNotSupportedException {
		return (PhysicalOperator) super.clone();
	}

	protected void cloneHelper(PhysicalOperator op) {
		resultType = op.resultType;
		originalLocations.addAll(op.originalLocations);
	}

	/**
	 * @param physicalPlan
	 */
	public void setParentPlan(PhysicalPlan physicalPlan) {
		parentPlan = physicalPlan;
	}

	public Log getLogger() {
		return log;
	}

	public static void setPigLogger(PigLogger logger) {
		pigLogger = logger;
	}

	public static PigLogger getPigLogger() {
		return pigLogger;
	}

	public static class OriginalLocation implements Serializable {

		private static final long serialVersionUID = 5439569033285571363L;

		private String alias;

		private int line;

		private int offset;

		public OriginalLocation(String alias, int line, int offset) {
			super();
			this.alias = alias;
			this.line = line;
			this.offset = offset;
		}

		public String getAlias() {
			return alias;
		}

		public int getLine() {
			return line;
		}

		public int getOffset() {
			return offset;
		}

		public String toString() {
			return alias + "[" + line + "," + offset + "]";
		}
	}
}
