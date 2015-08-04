package org.apache.storm.executionengine.topologyLayer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Utils;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.storm.executionengine.topologyLayer.StormLauncher.Output;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.PigContext;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;

public class TopologyBolt extends BaseRichBolt implements Output {
	private static final Log LOG = LogFactory.getLog(TopologyBolt.class);
	private static final long serialVersionUID = 1L;
	private TopologyOperator topologyOperator = null;
	private PigContext pigContext = null;
	private Map conf = null;
	private String boltID = null;
	private int parallelism = 1;
	private List<String> outputStreams = null;

	private TopologyContext context = null;
	// key:需要往下一级bolt发送数据的节点 value:该节点发送数据的stream列表
	private Map<PhysicalOperator, List<String>> operatorStreams = null;
	// key:流入bolt的数据流名称 value:产生该流的partition运算符
	private Map<String, POPartition> streamToPartiton = null;
	// key:流入bolt的数据流名称 value:需要处理该流的数据的root运算符
	private Map<String, List<PhysicalOperator>> streamToRootOperators = null;
	// key:流入bolt的数据流名称 value:通过这些运算符将数据发往下一级bolt（包括叶子节点或者非叶子节点）
	private Map<String, HashSet<PhysicalOperator>> streamToLeaves = null;

	private ProcessorOperatorPlan pp = new ProcessorOperatorPlan();

	// private ProcessorOperator rootPO = null;
	private Set<ProcessorOperator> rootPOList = null;

	private TopologyOperator to = null;

	private OutputCollector co = null;
	private String coLock = null;

	public TopologyBolt(TopologyOperator to, PigContext pigContext) {
		this.topologyOperator = to;
		this.pigContext = pigContext;
		List<POPartition> partitions = this.topologyOperator
				.getInputPartitions();
		if (partitions != null) {
			evalParallelism(partitions);
		}

		this.outputStreams = StormLauncher
				.getOutPutStreams(this.topologyOperator);
		this.operatorStreams = StormLauncher
				.buildOutputRelations(this.topologyOperator);

		this.streamToRootOperators = new HashMap<String, List<PhysicalOperator>>();
		this.streamToPartiton = new HashMap<String, POPartition>();
		List<String> rootAliasList;
		for (POPartition p : to.getInputPartitions()) {
			String streamID = StormLauncher.getStreamID(p);
			String alias = p.getAlias();
			this.streamToPartiton.put(streamID, p);
			rootAliasList = to.getPartitionSuccessors().get(alias);
			for (String rootAlias : rootAliasList) {
				List<PhysicalOperator> rootOperators = this.streamToRootOperators
						.get(streamID);
				if (rootOperators == null) {
					rootOperators = new ArrayList<PhysicalOperator>();
					this.streamToRootOperators.put(streamID, rootOperators);
				}
				rootOperators.add(to.topoPlan.getOperator(rootAlias));
			}
		}

		this.streamToLeaves = new HashMap<String, HashSet<PhysicalOperator>>();
		Iterator<String> itStreams = this.streamToRootOperators.keySet()
				.iterator();
		while (itStreams.hasNext()) {
			String stream = (String) itStreams.next();
			HashSet<PhysicalOperator> leafSet = new HashSet<PhysicalOperator>();
			this.streamToLeaves.put(stream, leafSet);
			for (PhysicalOperator root : this.streamToRootOperators.get(stream)) {
				for (PhysicalOperator leaf : getLeaves(to.topoPlan, root)) {
					leafSet.add(leaf);
				}
			}

			// 有些partition作用在非叶子节点上，因此这些节点计算完成后也可能直接将数据发往下一级bolt
			for (POPartition p : to.getOutputPartitions()) {
				String source = p.getSourceAlias();
				PhysicalOperator op = to.topoPlan.getOperator(source);

				// 通过successor的数量来判断是否为非叶子节点
				List<PhysicalOperator> successors = to.topoPlan
						.getSuccessors(op);
				if (successors != null && successors.size() > 0) {
					for (PhysicalOperator root : this.streamToRootOperators
							.get(stream)) {
						if (root == op || to.topoPlan.pathExists(root, op)) {
							this.streamToLeaves.get(stream).add(op);
						}
					}
				}
			}
		}
		this.to = to;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		coLock = new String("lock");
		this.conf = stormConf;
		this.context = context;
		this.co = collector;
		try {
			StormLauncher.initFunc(this.topologyOperator.topoPlan, this.conf);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		try {
			pp = StormLauncher.buildPPPlan(to, operatorStreams, this);

		} catch (Exception e) {
			throw new RuntimeException("Excpetion when build processor plan.",
					e);
		}

		Iterator<ProcessorOperator> itp = pp.iterator();
		while (itp.hasNext()) {
			itp.next().prepare(stormConf, context.getSharedExecutor());
		}
		// TODO rootPO = pp.getRoots().get(0);
		rootPOList = new HashSet<ProcessorOperator>();
	}

	public void execute(backtype.storm.tuple.Tuple input) {
		String inputStream = input.getSourceStreamId();
		//
		List<ProcessorOperator> rootList = pp.getRoots();
		if (rootList.size() == 1) {
			// rootPO = pp.getRoots().get(0);
			rootPOList.add(pp.getRoots().get(0));
		} else {
			for (ProcessorOperator po : rootList) {
				Map<String, Set<String>> boltInputMap = po
						.getBoltLevalInputOperators();
				if (!boltInputMap.isEmpty()) {
					Set<String> keySet = boltInputMap.keySet();
					for (String key : keySet) {
						if (inputStream.endsWith(key)) {
							// rootPO = po;
							// break;
							rootPOList.add(po);
						}
					}
				}
			}
		}
		try {
			for (ProcessorOperator rootPO : rootPOList) {
				rootPO.feedData(this.streamToPartiton.get(inputStream)
						.getAlias(), (DataBag) input.getValue(0));
			}
		} catch (Exception e) {
			LOG.info("e Exception when process bolt data.Input:" + input, e);
		} finally {
			rootPOList.clear();
		}
	}

	private List<PhysicalOperator> getLeaves(PhysicalPlan plan,
			PhysicalOperator operatorOnThePath) {
		List<PhysicalOperator> leaves = new ArrayList<PhysicalOperator>();
		for (PhysicalOperator leaf : plan.getLeaves()) {
			if ((leaf != operatorOnThePath)
					&& (!plan.pathExists(operatorOnThePath, leaf)))
				continue;
			leaves.add(leaf);
		}

		return leaves;
	}

	public List<String> getInputStream() {
		Iterator<String> it = streamToRootOperators.keySet().iterator();
		List<String> streams = new ArrayList<String>();
		while (it.hasNext()) {
			streams.add(it.next());
		}

		return streams;
	}

	private void evalParallelism(List<POPartition> partitions) {
		for (POPartition p : partitions)
			if (p.getRequestedParallelism() > this.parallelism)
				this.parallelism = p.getRequestedParallelism();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (this.outputStreams == null) {
			return;
		}
		for (String outputStream : this.outputStreams)
			declarer.declareStream(outputStream, new Fields(
					new String[] { "data" }));
	}

	public void cleanup() {
		Iterator<ProcessorOperator> itp = pp.iterator();
		while (itp.hasNext()) {
			itp.next().cleanup();
		}

		try {
			StormLauncher.cleanFunc(this.topologyOperator.topoPlan);
		} catch (Exception e) {
			throw new RuntimeException("Exception when clean function.", e);
		}
	}

	public String getBoltID() {
		if (this.boltID == null) {
			this.boltID = Utils.getBoltID(this.topologyOperator);
		}

		return this.boltID;
	}

	public int getParallelism() {
		if (this.parallelism == -1) {
			this.parallelism = 1;
		}

		return this.parallelism;
	}

	@Override
	public void emit(String streamID, List<Object> tuple) {
		if (null != tuple) {
			synchronized (coLock) {
				co.emit(streamID, tuple);
			}
		} else {
			LOG.error("topology bolt,null == tuple");
		}
	}
}
