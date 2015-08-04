package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.TapFunc;
import org.apache.pig.TapFuncWrapper;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.topologyLayer.Config;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.pen.util.ExampleTuple;

/**
 * The Tap operator which is used to load data.
 * 
 */
public class POTap extends PhysicalOperator {
	private static final Log log = LogFactory.getLog(POTap.class);
	/**
     * 
     */
	private static final long serialVersionUID = 1L;
	// The user defined tap function or a default tap function
	private transient TapFunc loader = null;
	// The FuncSpec on which the operator is based
	FuncSpec fs;
	// PigContext passed to us by the operator creator
	PigContext pc;
	// Indicates whether the loader setup is done or not
	boolean setUpDone = false;
	// Alias for the POTap
	private String signature;
	// flag
	private boolean isTmpLoad;

	private long limit = -1;
	// TODO
	private LogicalSchema scriptSchema;

	private Config config = null;

	private Map<String, String> context;

	public void setScriptSchema(LogicalSchema scriptSchema) {
		this.scriptSchema = scriptSchema;
	}

	public POTap(OperatorKey k) {
		this(k, -1, null);
	}

	public POTap(OperatorKey k, FuncSpec fs) {
		this(k, -1, fs);
	}

	public POTap(OperatorKey k, int rp, FuncSpec fs) {
		super(k, rp);
		this.fs = fs;
		this.context = new HashMap<String, String>();
	}

	public POTap(OperatorKey k, TapFunc lf) {
		this(k);
		this.loader = lf;
		this.context = new HashMap<String, String>();
	}

	/**
	 * Set up the loader by 1) Instantiating the tap func 2) Opening an input
	 * stream to the specified file and 3) Binding to the input stream at the
	 * specified offset.
	 * 
	 * @throws IOException
	 */
	public void setUp(Config conf) throws IOException {
		loader = new TapFuncWrapper(
				(TapFunc) PigContext.instantiateFuncFromSpec(fs), null);
		loader.setScriptSchema(scriptSchema);
		// TODO
		Map<String, String> tempMap = conf.asStringMap();
		tempMap.putAll(getContext());
		Config config = new Config(tempMap, fs.getClassName(), this.getAlias());
		//
		loader.open(config);
		this.config = config;
		setUpDone = true;
	}

	/**
	 * At the end of processing, the inputstream is closed using this method
	 * 
	 * @throws IOException
	 */
	public void tearDown() throws IOException {
		setUpDone = false;
		// TODO
		loader.close();
	}

	/**
	 * The main method used by this operator's successor to read tuples from the
	 * specified file using the specified tap function.
	 * 
	 * @return Whatever the loader returns A null from the loader is indicative
	 *         of EOP and hence the tearDown of connection
	 */
	@Override
	public Result getNextTuple() throws ExecException {
		if (!setUpDone && fs != null) {
			try {
				setUp(this.config);
			} catch (IOException ioe) {
				int errCode = 2081;
				String msg = "Unable to setup the tap function.";
				throw new ExecException(msg, errCode, PigException.BUG, ioe);
			}
			setUpDone = true;
		}
		Result res = new Result();
		try {
			res.result = loader.getNext();
			if (res.result == null) {
				res.returnStatus = POStatus.STATUS_EOP;
				// tearDown();
			} else {
				res.returnStatus = POStatus.STATUS_OK;
				log.debug("Tap output:" + alias + ":" + (Tuple) res.result);
			}
		} catch (IOException e) {
			log.error("Received error from tap function: " + e);
			return res;
		}
		return res;
	}

	public Result processDataBags(DataBag... bags) throws ExecException {
		Result r = new Result();
		r.result = bags[0];
		r.returnStatus = POStatus.STATUS_OK;
		return r;
	}

	@Override
	public Result getNextDataBag() throws ExecException {
		// for unit test
		if (this.inputBagsAttached) {
			return super.getNextDataBag();
		}

		Result ret = null;
		// DataBag tmpBag = BagFactory.getInstance().newDefaultBag();
		DataBag tmpBag = new NonSpillableDataBag();
		for (ret = getNextTuple(); ret.returnStatus != POStatus.STATUS_EOP; ret = getNextTuple()) {
			if (ret.returnStatus == POStatus.STATUS_ERR) {
				return ret;
			}
			tmpBag.add((Tuple) ret.result);
		}
		ret.result = tmpBag;
		ret.returnStatus = (tmpBag.size() == 0) ? POStatus.STATUS_EOP
				: POStatus.STATUS_OK;
		// TODO
		try {
			tearDown();
		} catch (IOException e) {
			log.error("Unable to tearDown the tap function: " + e);
		}
		//
		return ret;
	}

	@Override
	public String name() {
		return (fs != null) ? getAliasString() + "Tap" + "(" + fs.toString()
				+ ")" + " - " + mKey.toString() : getAliasString() + "Tap"
				+ "(" + "DummyFil:DummyLdr" + ")" + " - " + mKey.toString();
	}

	@Override
	public boolean supportsMultipleInputs() {
		return false;
	}

	@Override
	public boolean supportsMultipleOutputs() {
		return true;
	}

	@Override
	public void visit(PhyPlanVisitor v) throws VisitorException {
		v.visitTap(this);
	}

	public FuncSpec getFuncSpec() {
		return fs;
	}

	public void setFuncSpec(FuncSpec fs) {
		this.fs = fs;
	}

	public void setIsTmpLoad(boolean tmp) {
		isTmpLoad = tmp;
	}

	public boolean isTmpLoad() {
		return isTmpLoad;
	}

	public PigContext getPc() {
		return pc;
	}

	public void setPc(PigContext pc) {
		this.pc = pc;
	}

	public String getSignature() {
		return signature;
	}

	public void setSignature(String signature) {
		this.signature = signature;
	}

	public TapFunc getTapFunc() {
		if (this.loader == null) {
			this.loader = (TapFunc) PigContext.instantiateFuncFromSpec(fs);
			this.loader.setUDFContextSignature(signature);
		}
		return this.loader;
	}

	public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
		if (illustrator != null) {
			if (!illustrator.ceilingCheck()) {
				((Result) in).returnStatus = POStatus.STATUS_EOP;
				return null;
			}
			if (illustrator.getSchema() == null
					|| illustrator.getSchema().size() <= ((Tuple) out).size()) {
				boolean hasNull = false;
				for (int i = 0; i < ((Tuple) out).size(); ++i)
					try {
						if (((Tuple) out).get(i) == null) {
							hasNull = true;
							break;
						}
					} catch (ExecException e) {
						hasNull = true;
						break;
					}
				if (!hasNull) {
					ExampleTuple tOut = new ExampleTuple((Tuple) out);
					illustrator.getLineage().insert(tOut);
					illustrator.addData((Tuple) tOut);
					illustrator.getEquivalenceClasses().get(eqClassIndex)
							.add(tOut);
					return tOut;
				} else
					return (Tuple) out;
			} else
				return (Tuple) out;
		} else
			return (Tuple) out;
	}

	public long getLimit() {
		return limit;
	}

	public void setLimit(long limit) {
		this.limit = limit;
	}

	public Map<String, String> getContext() {
		return context;
	}

	public void setContext(Map<String, String> context) {
		this.context = context;
	}

	public int getParallelism() {
		return this.getRequestedParallelism();
	}
}
