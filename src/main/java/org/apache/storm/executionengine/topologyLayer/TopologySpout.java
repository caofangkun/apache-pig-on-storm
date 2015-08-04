package org.apache.storm.executionengine.topologyLayer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POTap;
import org.apache.storm.executionengine.topologyLayer.StormLauncher.Output;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.SingleTupleBag;
import org.apache.pig.impl.PigContext;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class TopologySpout extends BaseRichSpout {
	private static final Log LOG = LogFactory.getLog(TopologySpout.class);
	private static final long serialVersionUID = 1L;
	private static final int DEFAULT_SPOUT_PARALLELISM = 1;
	private Map conf = null;
	private TopologyContext context = null;
	private SpoutOutputCollector collector = null;
	private TopologyOperator topologyOperator = null;
	private PigContext pigContext = null;
	private PhysicalPlan physicalPlan = null;
	private POTap tap = null;
	private int parallelism = DEFAULT_SPOUT_PARALLELISM;
	private List<String> outputStreams = null;
	private Map<PhysicalOperator, List<String>> operatorStreams = null;
	private TopologyOperator to = null;
	private ProcessorOperatorPlan pp = new ProcessorOperatorPlan();
	private ProcessorOperator rootPO = null;

	public TopologySpout(TopologyOperator topoOperator, PigContext pigContext) {
		this.topologyOperator = topoOperator;
		this.pigContext = pigContext;
		this.physicalPlan = topoOperator.topoPlan;
		this.tap = (POTap) this.physicalPlan.getRoots().get(0);
		this.outputStreams = StormLauncher
				.getOutPutStreams(this.topologyOperator);
		this.operatorStreams = StormLauncher
				.buildOutputRelations(this.topologyOperator);
		if (this.tap.getRequestedParallelism() > 0) {
			this.parallelism = this.tap.getRequestedParallelism();
		}
		this.to = topoOperator;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf = conf;
		this.context = context;
		this.collector = collector;

		// this.physicalPlan.remove(this.tap);
		try {
			Config config = new Config(conf, this.tap.getFuncSpec()
					.getClassName(), tap.getAlias());
			this.tap.setUp(config);
		} catch (IOException e1) {
			throw new RuntimeException("Exception when open TapFunc.", e1);
		}

		try {
			StormLauncher.initFunc(this.physicalPlan, conf);
		} catch (Exception e) {
			throw new RuntimeException("Exception when init function.", e);
		}

		final SpoutOutputCollector co = collector;
		try {
			pp = StormLauncher.buildPPPlan(to, operatorStreams, new Output() {
				private static final long serialVersionUID = 1L;

				public void emit(String streamID, List<Object> tuple) {
					co.emit(streamID, tuple);
				}
			});
		} catch (Exception e) {
			throw new RuntimeException("Excpetion when build processor plan.",
					e);
		}

		Iterator<ProcessorOperator> itp = pp.iterator();
		while (itp.hasNext()) {
			itp.next().prepare(conf, context.getSharedExecutor());
		}

		rootPO = pp.getRoots().get(0);
	}

	private void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			LOG.info(e);
		}
	}

	@Override
	public void nextTuple() {
		Result rs;
		try {
			rs = this.tap.getNextTuple();
		} catch (Exception e1) {
			LOG.error("Exception when load data.", e1);
			return;
		}
		DataBag bag = null;
		if (rs.returnStatus == org.apache.storm.executionengine.physicalLayer.POStatus.STATUS_OK) {

			org.apache.pig.data.Tuple resultTuple = (org.apache.pig.data.Tuple) rs.result;
			if (resultTuple == null || resultTuple.size() == 0) {
				LOG.error("POTap returns OK status result, but the result tuple is null or empty.");
				return;
			}
			bag = new SingleTupleBag(resultTuple);
		} else if (rs.returnStatus == org.apache.storm.executionengine.physicalLayer.POStatus.STATUS_ERR) {
			LOG.error("Error when get data from func." + rs.toString());
			return;
		} else if (rs.returnStatus == org.apache.storm.executionengine.physicalLayer.POStatus.STATUS_NULL) {
			return;
		} else {
			return;
		}
		try {
			rootPO.feedData(null, bag);
		} catch (Exception e) {
			LOG.info("Exception when process spout data.DataBag:" + bag, e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (this.outputStreams == null) {
			return;
		}
		for (String outputStream : outputStreams) {
			declarer.declareStream(outputStream, new Fields("data"));
		}
	}

	@Override
	public void close() {
		Iterator<ProcessorOperator> itp = pp.iterator();
		while (itp.hasNext()) {
			itp.next().cleanup();
		}

		Exception ex = null;
		try {
			StormLauncher.cleanFunc(this.physicalPlan);
		} catch (Exception e) {
			ex = e;
		}

		try {
			this.tap.tearDown();
		} catch (Exception e) {
			ex = e;
		}
		if (ex != null) {
			throw new RuntimeException("Exception when clos spolt.", ex);
		}
	}

	public String getSpoutID() {

		return "Spout_" + tap.getAlias();
	}

	public int getParallelism() {
		return this.parallelism;
	}
}
