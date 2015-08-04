package org.apache.storm.executionengine.topologyLayer;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.Manageable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.storm.Utils;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.storm.executionengine.topologyLayer.plans.TopologyOperatorPlan;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.SingleTupleBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class StormLauncher implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(StormLauncher.class);
	private Map<TopologyOperator, TopologySpout> osMap = new HashMap<TopologyOperator, TopologySpout>();

	private transient Map<TopologyOperator, Object> opToToBoltSpout = new HashMap<TopologyOperator, Object>();
	private transient Map<Object, Object> boltSpoutToDeclare = new HashMap<Object, Object>();
	private transient Map<String, TopologyBolt> boltMap = new HashMap<String, TopologyBolt>();
	private transient Map<String, TopologySpout> spoutMap = new HashMap<String, TopologySpout>();
	private static boolean debug = false;

	private LocalCluster localCluster = null;

	public void launchStorm(TopologyOperatorPlan topoOperPlan,
			PigContext pigContext, Map conf) throws AlreadyAliveException,
			InvalidTopologyException {
		debug = Utils.getBoolean(conf, "debug", false);
		StormTopology st = buildTopology(topoOperPlan, pigContext);
		String execType = System.getProperty("execType");
		if ("local".equalsIgnoreCase(execType)) {
			launchLocal(topoOperPlan, pigContext, conf, st);
		} else {
			StormSubmitter.submitTopology(conf.get("topology.name").toString(),
					conf, st);
		}
	}

	public void launchStormOnYarn(TopologyOperatorPlan topoOperPlan,
			PigContext pigContext, Map conf) throws AlreadyAliveException,
			InvalidTopologyException {
		debug = Utils.getBoolean(conf, "debug", false);
		StormTopology st = buildTopology(topoOperPlan, pigContext);
		String execType = System.getProperty("execType");
		if ("local".equalsIgnoreCase(execType)) {
			launchLocal(topoOperPlan, pigContext, conf, st);
		} else {
			com.yahoo.storm.yarn.Client.submitTopology(conf
					.get("topology.name").toString(), conf, st, null);
		}
	}

	private void launchLocal(TopologyOperatorPlan topoOperPlan,
			PigContext pigContext, Map conf, StormTopology st) {
		LocalCluster lc = new LocalCluster();
		lc.submitTopology(conf.get("topology.name").toString(), conf, st);
	}

	public void lauchWebLocal(TopologyOperatorPlan topoOperPlan,
			PigContext pigContext, Map conf) {
		debug = Utils.getBoolean(conf, "debug", false);
		StormTopology st = buildTopology(topoOperPlan, pigContext);
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology(conf.get("topology.name").toString(), conf,
				st);
		this.localCluster = localCluster;
	}

	public void stopWebLocal() {
		this.localCluster.shutdown();
	}

	public StormTopology buildTopology(TopologyOperatorPlan topoOperPlan,
			PigContext pigContext) {
		if ((topoOperPlan == null) || (topoOperPlan.size() == 0)) {
			LOG.error("TopologyOperatorPlan is zero.");
			return null;
		}

		TopologyBuilder tb = new TopologyBuilder();
		Iterator<TopologyOperator> it = topoOperPlan.iterator();
		while (it.hasNext()) {
			TopologyOperator to = (TopologyOperator) it.next();
			if (to.hasTap()) {
				TopologySpout spout = new TopologySpout(to, pigContext);
				SpoutDeclarer sd = tb.setSpout(spout.getSpoutID(), spout,
						Integer.valueOf(spout.getParallelism()));
				this.opToToBoltSpout.put(to, spout);
				this.boltSpoutToDeclare.put(spout, sd);
				spoutMap.put(spout.getSpoutID(), spout);
			} else {
				TopologyBolt bolt = new TopologyBolt(to, pigContext);
				BoltDeclarer bd = tb.setBolt(bolt.getBoltID(), bolt,
						Integer.valueOf(bolt.getParallelism()));
				this.opToToBoltSpout.put(to, bolt);
				this.boltSpoutToDeclare.put(bolt, bd);
				boltMap.put(bolt.getBoltID(), bolt);
			}
		}

		it = topoOperPlan.iterator();
		while (it.hasNext()) {
			TopologyOperator to = (TopologyOperator) it.next();

			if (!to.hasTap()) {
				BoltDeclarer decalare = (BoltDeclarer) this.boltSpoutToDeclare
						.get(this.opToToBoltSpout.get(to));
				for (POPartition p : to.getInputPartitions()) {
					String inputStreamID = getStreamID(p);
					String sourceAlias = p.getSourceAlias();
					TopologyOperator inputTO = topoOperPlan
							.getTOByPOAlias(sourceAlias);

					Object o = this.opToToBoltSpout.get(inputTO);

					if ((o instanceof TopologySpout))
						decalare.customGrouping(
								((TopologySpout) o).getSpoutID(),
								inputStreamID, new ExpressionShuffleGrouping(p));
					else {
						decalare.customGrouping(((TopologyBolt) o).getBoltID(),
								inputStreamID, new ExpressionShuffleGrouping(p));
					}
				}
			}
		}

		StormTopology st = tb.createTopology();
		return st;
	}

	public Map<String, TopologyBolt> getBoltMap() {
		return this.boltMap;
	}

	public Map<String, TopologySpout> getSpoutMap() {
		return this.spoutMap;
	}

	public static void initFunc(PhysicalPlan plan, Map stormConf)
			throws VisitorException {
		PrepareFuncVisitor pv = new PrepareFuncVisitor(
				plan,
				new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan),
				stormConf);
		pv.visit();
	}

	public static void cleanFunc(PhysicalPlan plan) throws VisitorException {
		CleanupFuncVisitor pv = new CleanupFuncVisitor(plan,
				new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
		pv.visit();

	}

	public static boolean processBoltData(List<String> outputStreams,
			OutputCollector collector, PhysicalOperator startNode) {
		Result ret = null;
		try {
			ret = startNode.getNextDataBag();
			if (debug) {
				LOG.info(String
						.format("Bolt debug,after processed by node:%s the value is:%s",
								startNode.getAlias() == null ? startNode
										.toString() : startNode.getAlias(), ret
										.toString()));
			}
		} catch (ExecException e) {
			LOG.error("Error when process data.", e);
			return false;
		}

		if (ret.returnStatus == POStatus.STATUS_OK) {
			// some leaves in the physical don't need to output to next bolt
			if (outputStreams != null) {
				for (String stream : outputStreams) {
					emit(stream, collector, ret);
				}

				// List<Object> bagTuple = new ArrayList<Object>();
				// DataBag bag = (DataBag) ret.result;
				// bagTuple.add(bag);
				// Iterator<org.apache.pig.data.Tuple> it = bag.iterator();
				// while (it.hasNext()) {
				// org.apache.pig.data.Tuple tuple = it.next();
				// DataBag outputBag = new SingleTupleBag(tuple);
				// List<Object> outputTuple = new ArrayList<Object>();
				// outputTuple.add(outputBag);
				// for (String stream : outputStream) {
				// collector.emit(stream, outputTuple);
				// }
				// }
			}

			return true;
		}

		if (ret.returnStatus == POStatus.STATUS_NULL) {
			return true;
		}
		if (ret.returnStatus == POStatus.STATUS_ERR) {
			LOG.error("Error when process data.Leaf physicaloperator is:"
					+ startNode);
		}
		return false;
	}

	public static boolean processSpoutData(List<String> outputStream,
			SpoutOutputCollector collector, PhysicalOperator startNode) {
		Result ret = null;
		try {
			ret = startNode.getNextDataBag();
		} catch (ExecException e) {
			LOG.error("Error when process data.", e);
			return false;
		}

		if (ret.returnStatus == POStatus.STATUS_OK) {
			// some leaves in the physical don't need to output to next bolt
			if (outputStream != null) {
				for (String stream : outputStream) {
					emit(stream, collector, ret);
				}
			}

			return true;
		}

		if (ret.returnStatus == POStatus.STATUS_NULL) {
			return true;
		}

		LOG.error(String
				.format("Error when process spout data. Starting operator is:%s,result is:%s",
						startNode, ret.toString()));
		return false;
	}

	private static void emit(String outputStream, OutputCollector collector,
			Result ret) {
		if (debug) {
			LOG.info("Bolt emit,OutputStream:" + outputStream + ",Result:"
					+ ret);
		}

		List<Object> bagTuple = new ArrayList<Object>();
		DataBag bag = (DataBag) ret.result;
		bagTuple.add(bag);
		Iterator<org.apache.pig.data.Tuple> it = bag.iterator();
		while (it.hasNext()) {
			org.apache.pig.data.Tuple tuple = it.next();
			DataBag outputBag = new SingleTupleBag(tuple);
			List<Object> outputTuple = new ArrayList<Object>();
			outputTuple.add(outputBag);
			collector.emit(outputStream, outputTuple);
		}
	}

	public static void emit(String outputStream,
			SpoutOutputCollector collector, Result ret) {
		if (debug) {
			LOG.info("Spout emit,OutputStream:" + outputStream + ",Result:"
					+ ret);
		}

		List<Object> bagTuple = new ArrayList<Object>();
		DataBag bag = (DataBag) ret.result;
		bagTuple.add(bag);
		Iterator<org.apache.pig.data.Tuple> it = bag.iterator();
		while (it.hasNext()) {
			org.apache.pig.data.Tuple tuple = it.next();
			DataBag outputBag = new SingleTupleBag(tuple);
			List<Object> outputTuple = new ArrayList<Object>();
			outputTuple.add(outputBag);
			collector.emit(outputStream, outputTuple);
		}
	}

	public static List<String> getOutPutStreams(TopologyOperator to) {
		List<String> streams = new ArrayList<String>();

		List<POPartition> partitions = to.getOutputPartitions();
		if (partitions != null && partitions.size() > 0) {
			for (POPartition po : partitions) {
				streams.add(getStreamID(po));
			}
		} else {
			streams.add("default");
		}

		return streams;
	}

	public static ProcessorOperatorPlan buildPPPlan(TopologyOperator to,
			Map<PhysicalOperator, List<String>> operatorStreams,
			Output collector) throws TopologyCompilerException, PlanException,
			VisitorException {
		ProcessPlanCompiler pc = new ProcessPlanCompiler(to);
		pc.compile();
		ProcessorOperatorPlan pp = pc.getProcessorOperatorPlan();
		final Map<PhysicalOperator, List<String>> os = operatorStreams;
		final Output output = collector;
		Iterator<ProcessorOperator> itp = pp.iterator();
		while (itp.hasNext()) {
			final ProcessorOperator po = itp.next();
			po.setBoltLevalInputOperators(getBoltLevalMap(po,
					to.getPartitionSuccessors()));
			po.setBoltLevalOutputOperators(getBoltLevalList(po, to));
			po.setBoltLevalEmitter(new Emitter() {
				private static final long serialVersionUID = 1L;
				private Object emitLock = new Object();

				public void emit(String fromAlias, DataBag bag) {
					synchronized (emitLock) {
						List<String> outputStreams = os.get(po.getInnerPlan()
								.getOperator(fromAlias));
						if (outputStreams != null && outputStreams.size() > 0) {
							Iterator<org.apache.pig.data.Tuple> it = null;
							org.apache.pig.data.Tuple tuple = null;
							DataBag outputBag = null;
							List<Object> outputTuple = null;

							for (String stream : outputStreams) {
								// List<Object> bagTuple = new
								// ArrayList<Object>();
								// bagTuple.add(bag);
								it = bag.iterator();
								while (it.hasNext()) {
									tuple = it.next();
									outputBag = new SingleTupleBag(tuple);
									outputTuple = new ArrayList<Object>(1);
									outputTuple.add(outputBag);
									output.emit(stream, outputTuple);
								}
							}
						}
					}
				}
			});
		}
		return pp;
	}

	private static List<String> getBoltLevalList(ProcessorOperator po,
			TopologyOperator to) {
		List<String> list = new ArrayList<String>();
		Iterator<POPartition> itp = to.getOutputPartitions().iterator();
		POPartition p = null;
		while (itp.hasNext()) {
			p = itp.next();
			if (po.getInnerPlan().getOperator(p.getSourceAlias()) != null) {
				list.add(p.getSourceAlias());
			}
		}

		return list;
	}

	private static Map<String, List<String>> getBoltLevalMap(
			ProcessorOperator po, Map<String, List<String>> allBoltLevalMap) {
		Map<String, List<String>> map = new HashMap<String, List<String>>();

		Iterator<String> its = allBoltLevalMap.keySet().iterator();
		String alias = null;
		while (its.hasNext()) {
			alias = its.next();
			List<String> lists = allBoltLevalMap.get(alias);
			Iterator<PhysicalOperator> itp = po.getInnerPlan().iterator();
			while (itp.hasNext()) {
				if (lists.contains(itp.next().getAlias())) {
					map.put(alias, lists);
					break;
				}
			}
		}

		return map;
	}

	public static interface Output extends Serializable {
		void emit(String streamID, List<Object> tuple);
	}

	public static Map<PhysicalOperator, List<String>> buildOutputRelations(
			TopologyOperator to) {
		Map<PhysicalOperator, List<String>> map = new HashMap<PhysicalOperator, List<String>>();
		if (to.topoPlan.size() == 0) {
			return map;
		}

		for (PhysicalOperator po : to.topoPlan.getLeaves()) {
			map.put(po, null);
		}

		List<POPartition> partitions = to.getOutputPartitions();
		if (partitions != null && partitions.size() > 0) {
			for (POPartition pop : partitions) {
				PhysicalOperator source = pop.getSourceOperator();
				if (!map.containsKey(source)) {
					map.put(source, null);
				}

				List<String> streams = map.get(source);
				if (streams == null) {
					streams = new ArrayList<String>();
					map.put(source, streams);
				}

				String stream = getStreamID(pop);
				streams.add(stream);
			}
		}

		return map;
	}

	public static String getStreamID(POPartition pt) {
		return pt.getSourceAlias() + "->" + pt.getAlias();
	}

	private class ExpressionShuffleGrouping implements CustomStreamGrouping {
		private static final long serialVersionUID = 1L;
		private POPartition partionOperator = null;
		private List<Integer> targetTasks = null;
		private List<PhysicalPlan> plans = null;
		private List<Object> tmp = null;

		public ExpressionShuffleGrouping(POPartition partionOperator) {
			this.partionOperator = partionOperator;
			this.plans = partionOperator.getPlans();
			this.tmp = new ArrayList<Object>(this.plans.size());
			for (int i = 0; i < this.plans.size(); i++) {
				this.tmp.add(null);
			}
		}

		@Override
		public void prepare(WorkerTopologyContext context,
				GlobalStreamId stream, List<Integer> targetTasks) {
			this.targetTasks = targetTasks;
			Map stormConf = null;
			try {
				stormConf = getStormConf(context);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			initFunc(stormConf);
		}

		@Override
		public List<Integer> chooseTasks(int taskId, List<Object> values) {
			DataBag bag = (DataBag) values.get(0);
			org.apache.pig.data.Tuple tuple = bag.iterator().next();
			int hashCode = getPartitionedTuple(tuple).hashCode();
			if (hashCode != Integer.MIN_VALUE) {
				hashCode = Math.abs(hashCode);
			} else {
				hashCode = 0;
			}

			int choosenTaskIndex = hashCode % this.targetTasks.size();

			List<Integer> tasks = new ArrayList<Integer>();
			tasks.add(targetTasks.get(choosenTaskIndex));

			if (debug) {
				LOG.info("Tuple:" + tuple + " shuffled to task:" + tasks);
			}
			return tasks;
		}

		private Map getStormConf(WorkerTopologyContext context)
				throws SecurityException, NoSuchFieldException,
				IllegalArgumentException, IllegalAccessException {
			Class<?> clazz = context.getClass();
			while (clazz != GeneralTopologyContext.class) {
				clazz = clazz.getSuperclass();
			}
			Field confField = clazz.getDeclaredField("_stormConf");
			confField.setAccessible(true);
			return (Map) confField.get(context);
		}

		private void initFunc(Map stormConf) {
			try {
				for (PhysicalPlan plan : plans) {
					PrepareFuncVisitor pv = new PrepareFuncVisitor(
							plan,
							new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(
									plan), stormConf);
					pv.visit();
				}
			} catch (VisitorException e) {
				throw new RuntimeException(
						"Exception when preparing evalfunc at partition prepare phase, partition operator:"
								+ this.partionOperator, e);
			}
		}

		private Object getPartitionedTuple(org.apache.pig.data.Tuple tuple) {
			for (int i = 0; i < plans.size(); i++) {
				PhysicalPlan plan = plans.get(i);
				plan.attachInput(tuple);

				try {
					PhysicalOperator leaf = plan.getLeaves().get(0);
					Object o = null;
					try {
						o = leaf.getNext(leaf.getResultType()).result;
					} finally {
						leaf.recursiveDetachInput();
					}
					if (debug) {
						LOG.info("Partitioned result:" + o + ", Input tuple:"
								+ tuple);
					}
					if (o == null) {
						throw new RuntimeException(
								"Eval result of partition expression is  null. Input tuple:"
										+ tuple + ",expression:" + plan);
					}
					this.tmp.set(i, o);
				} catch (ExecException e) {
					throw new RuntimeException(
							"Exception when eval partition expressioin,partition:"
									+ this.partionOperator + ",Plan:" + plan
									+ ",Tuple:" + tuple, e);
				}
			}
			return tmp;
		}
	}

	private static class PrepareFuncVisitor extends PhyPlanVisitor {
		private Map stormConf = null;

		public PrepareFuncVisitor(PhysicalPlan plan,
				PlanWalker<PhysicalOperator, PhysicalPlan> walker, Map stormConf) {
			super(plan, walker);
			this.stormConf = stormConf;
		}

		public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
			if (userFunc.getFunc() instanceof Manageable) {
				try {
					((Manageable) userFunc.getFunc()).prepare(new Config(
							this.stormConf, userFunc.getFuncSpec()
									.getClassName(), userFunc
									.getWorkinOnOperatorAlias()));
				} catch (ExecException e) {
					LOG.error("Exception occur with Func visited in prepare:"
							+ userFunc, e);
					throw new VisitorException(e);
				}
			}
		}

		public void visitComparisonFunc(POUserComparisonFunc compFunc)
				throws VisitorException {
			if (compFunc.getFunc() instanceof Manageable) {
				try {
					((Manageable) compFunc.getFunc()).prepare(new Config(
							this.stormConf, compFunc.getFuncSpec()
									.getClassName(), compFunc
									.getWorkinOnOperatorAlias()));
				} catch (ExecException e) {
					LOG.error("Exception occur with Func visited in prepare:"
							+ compFunc, e);
					throw new VisitorException(e);
				}
			}
		}
	}

	private static class CleanupFuncVisitor extends PhyPlanVisitor {

		public CleanupFuncVisitor(PhysicalPlan plan,
				PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
			super(plan, walker);
		}

		public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
			if (userFunc instanceof Manageable) {
				try {
					((Manageable) userFunc).cleanup();
				} catch (ExecException e) {
					LOG.error("Exception occur with Func visited in cleanup:"
							+ userFunc, e);
					throw new VisitorException(e);
				}
			}
		}

		public void visitComparisonFunc(POUserComparisonFunc compFunc)
				throws VisitorException {
			if (compFunc instanceof Manageable) {
				try {
					((Manageable) compFunc).cleanup();
				} catch (ExecException e) {
					LOG.error("Exception occur with Func visited in cleanup:"
							+ compFunc, e);
					throw new VisitorException(e);
				}
			}
		}
	}
}