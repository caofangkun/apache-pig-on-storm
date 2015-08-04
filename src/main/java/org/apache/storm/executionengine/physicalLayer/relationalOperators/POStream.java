package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.DefaultAsyncEvalFunc;
import org.apache.pig.IAsyncEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.topologyLayer.Config;
import org.apache.storm.executionengine.topologyLayer.Emitter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.pen.util.ExampleTuple;

import com.google.common.base.Splitter;

public class POStream extends PhysicalOperator implements Window {
	private static final Log LOG = LogFactory.getLog(POStream.class);
	private static final long serialVersionUID = 2L;

	private StreamingCommand command; // Actual command to be run
	// private Properties properties;

	// PigContext passed to us by the operator creator
	public PigContext pc;

	private boolean initialized = false;

	protected BlockingQueue<Result> binaryOutputQueue = new ArrayBlockingQueue<Result>(
			1);

	protected BlockingQueue<Result> binaryInputQueue = new ArrayBlockingQueue<Result>(
			1);

	protected boolean allInputFromPredecessorConsumed = false;

	protected boolean allOutputFromBinaryProcessed = false;

	// ////////////////////////////////////////////////////////////////////////////////////////////

	private IAsyncEvalFunc func = null;

	// private Map<String, List<PhysicalOperator>> windowSeen;

	private Map<String, String> context;

	public POStream(OperatorKey k, ExecutableManager executableManager,
			StreamingCommand command, PigContext pc) {
		super(k);
		this.command = command;
		this.pc = pc;
		//
		// windowSeen = new HashMap<String, List<PhysicalOperator>>();
		context = new HashMap<String, String>();
		// boltClass=org.apache.pig.storm.sys.string.StrUpperCase#tdeTble=567#key=xxc
		String text = command.getExecutable();
		String blotClass = null;
		if (text.indexOf("=") == -1) {
			blotClass = text;
		} else {
			context = Splitter.on("#").withKeyValueSeparator("=").split(text);
			blotClass = context.get("boltClass");
		}
		//
		try {
			func = (IAsyncEvalFunc) PigContext
					.instantiateFuncFromSpec(blotClass);
		} catch (Exception e) {
			LOG.error(
					"Cannot instantiate the AsyncEvalFunc: "
							+ command.getExecutable(), e);
			func = new DefaultAsyncEvalFunc();
		}
	}

	public Map<String, String> getContext() {
		return context;
	}

	public Properties getShipCacheProperties() {
		return pc.getProperties();
	}

	/**
	 * Get the {@link StreamingCommand} for this <code>StreamSpec</code>.
	 * 
	 * @return the {@link StreamingCommand} for this <code>StreamSpec</code>
	 */
	public StreamingCommand getCommand() {
		return command;
	}

	public IAsyncEvalFunc getFunc() {
		return func;
	}

	public synchronized boolean getInitialized() {
		return initialized;
	}

	public synchronized void setInitialized(boolean initialized) {
		this.initialized = initialized;
	}

	public String toString() {
		return getAliasString() + "POStream" + "[" + command.toString() + "]"
				+ " - " + mKey.toString();
	}

	@Override
	public void visit(PhyPlanVisitor v) throws VisitorException {
		v.visitStream(this);
	}

	@Override
	public String name() {
		return toString();
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
	public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
		if (illustrator != null) {
			ExampleTuple tIn = (ExampleTuple) in;
			illustrator.getEquivalenceClasses().get(eqClassIndex).add(tIn);
			illustrator.addData((Tuple) out);
		}
		return (Tuple) out;
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

	// ////////////////////////////////////////////////////////////////////////////////////////

	public void attachInputBag(DataBag... bags) {
		DataBag bag = bags[0];
		try {
			if (bag != null) {
				processWindowInput(bag);
			}

		} catch (ExecException e) {
			throw new RuntimeException(
					"Excpetion when attach window input,Window:" + this
							+ ",input data Bag:" + bags[0], e);
		}

	}

	private void processWindowInput(DataBag bag) throws ExecException {
		func.execute(bag);
	}

	// ////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public boolean isWindowOperator() {
		return true;
	}

	@Override
	public void addEmitter(Emitter emitter) {
		if (func == null) {
			try {
				func = (IAsyncEvalFunc) PigContext
						.instantiateFuncFromSpec(command.getExecutable());
			} catch (Exception e) {
				LOG.error(
						"Cannot instantiate the AsyncEvalFunc: "
								+ command.getExecutable(), e);
				func = new DefaultAsyncEvalFunc();
			}
		}
		func.addEmitter(emitter);
	}

	@Override
	public void prepare(Map stormConf, ExecutorService executor, String streamId) {
		// TODO:
		Config config = new Config(stormConf, getFunc().getClass(), streamId);
		Map<String, String> conf = config.asStringMap();
		conf.putAll(getContext());

		Schema inputSchema = this.getInputs().get(0).getSchema();
		String inputAlias = this.getInputs().get(0).getAlias();
		String streamSchema = toIndentedString(inputSchema);
		String outputSchema = toIndentedString(this.getSchema());
		//
		conf.put("input_alias", inputAlias);
		conf.put("input_schema", streamSchema);
		conf.put("output_schema", outputSchema);
		func.prepare(conf, executor, streamId);
	}

	@Override
	public void cleanup() {
		func.cleanup();
	}

	private String toIndentedString(Schema inputSchema) {
		StringBuilder sb = new StringBuilder();
		try {
			stringifySchema(sb, inputSchema, DataType.BAG);
		} catch (FrontendException fee) {
			throw new RuntimeException("PROBLEM PRINTING SCHEMA");
		}
		return sb.toString();
	}

	// type can only be BAG or TUPLE
	private void stringifySchema(StringBuilder sb, Schema schema, byte type)
			throws FrontendException {

		if (type == DataType.TUPLE) {
			sb.append("(");
		} else if (type == DataType.BAG) {
			sb.append("(");
		}

		if (schema != null) {
			boolean isFirst = true;
			for (int i = 0; i < schema.size(); i++) {

				if (!isFirst) {
					sb.append(",");
				} else {
					isFirst = false;
				}

				FieldSchema fs = schema.getField(i);

				if (fs == null) {
					continue;
				}

				if (fs.alias != null) {
					if (fs.alias.indexOf("::") != -1) {
						String[] names = fs.alias.split("::");
						sb.append(names[1]);
						sb.append(" ");
					} else {
						sb.append(fs.alias);
						sb.append(" ");
					}
				}

				if (DataType.isAtomic(fs.type)) {
					String typeName = DataType.findTypeName(fs.type);
					if (typeName.equalsIgnoreCase("chararray")) {
						sb.append("string");
					} else {
						sb.append(typeName);
					}
				} else if ((fs.type == DataType.TUPLE)
						|| (fs.type == DataType.BAG)) {
					// safety net
					if (schema != fs.schema) {
						stringifySchema(sb, fs.schema, fs.type);
					} else {
						throw new AssertionError("Schema refers to itself "
								+ "as inner schema");
					}
				} else if (fs.type == DataType.MAP) {
					sb.append(DataType.findTypeName(fs.type) + "[");
					if (fs.schema != null)
						stringifySchema(sb, fs.schema, fs.type);
					sb.append("]");
				} else {
					sb.append(DataType.findTypeName(fs.type));
				}
			}
		}

		if (type == DataType.TUPLE) {
			sb.append(")");
		} else if (type == DataType.BAG) {
			sb.append(")");
		}

	}

}
