package org.apache.storm.executionengine.topologyLayer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POTap;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.Window;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class ProcessorOperator extends Operator<ProcessorPlanVistor> {
	private static final Log LOG = LogFactory.getLog(ProcessorOperator.class);
	private static final long serialVersionUID = 1L;
	private PhysicalPlan innerPlan = null;
	private ProcessorOperatorPlan outerPlan = null;
	private Map<String, Set<String>> inputMap = new HashMap<String, Set<String>>();
	private Map<String, Set<String>> outputMap = new HashMap<String, Set<String>>();

	private Set<String> startCalculatingPOSet = new TreeSet<String>();
	private transient Map<String, Set<ProcessorOperator>> aliasToNextProcessorOperatorMap = null;
	private transient Emitter boltLevalEmitter = null;
	private Map<String, Set<String>> boltLevalInputMap = new HashMap<String, Set<String>>();
	private Set<String> boltLevalOutputOperatorSet = new TreeSet<String>();

	private boolean isRootProcessorOperator = false;
	private boolean isWindow = false;
	private transient Window windowPO = null;
	private List<PhysicalOperator> roots = null;
	private boolean isTap = false;

	// private Map<String, List<PhysicalOperator>> windowInput;
	// private Map<String, List<PhysicalOperator>> windowOutput;
	private Object lock = new Object();

	public ProcessorOperator(OperatorKey k, ProcessorOperatorPlan ouoterPlan) {
		super(k);
		this.innerPlan = new PhysicalPlan();
		this.outerPlan = ouoterPlan;
		// this.windowInput = new HashMap<String, List<PhysicalOperator>>();
		// this.windowOutput = new HashMap<String, List<PhysicalOperator>>();
	}

	public boolean isWindowProcessor() {
		return this.isWindow;
	}

	public void setWindowProcessor(boolean isWindow) {
		this.isWindow = isWindow;
	}

	// public Map<String, List<PhysicalOperator>> getWindowInput() {
	// return windowInput;
	// }
	//
	// public void setWindowInput(Map<String, List<PhysicalOperator>>
	// windowInput) {
	// this.windowInput = windowInput;
	// }
	//
	// public Map<String, List<PhysicalOperator>> getWindowOutput() {
	// return windowOutput;
	// }
	//
	// public void setWindowOutput(Map<String, List<PhysicalOperator>>
	// windowOutput) {
	// this.windowOutput = windowOutput;
	// }

	// ---------------------------------------
	public void prepare(Map stormConf, ExecutorService executor) {
		this.aliasToNextProcessorOperatorMap = buildOutputAliasToProcessorOperatorMap();
		buildStartCalculatingPO();
		this.isRootProcessorOperator = this.outerPlan.getRoots().contains(this);
		roots = this.innerPlan.getRoots();

		if (roots.size() == 1 && roots.get(0) instanceof POTap) {
			this.isTap = true;
		}

		if (this.innerPlan.size() == 1
				&& this.innerPlan.getRoots().get(0) instanceof Window
				&& ((Window) this.innerPlan.getRoots().get(0))
						.isWindowOperator()) {
			windowPO = (Window) this.innerPlan.getRoots().get(0);
			this.isWindow = true;
			if (aliasToNextProcessorOperatorMap
					.containsKey(((PhysicalOperator) windowPO).getAlias())) {
				windowPO.addEmitter(new Emitter() {
					private static final long serialVersionUID = 1L;

					public void emit(String currentNode, DataBag bag)
							throws Exception {
						if (aliasToNextProcessorOperatorMap != null) {
							Iterator<ProcessorOperator> pos = aliasToNextProcessorOperatorMap
									.get(currentNode).iterator();
							while (pos.hasNext()) {
								ProcessorOperator po = pos.next();
								if (po != null) {
									po.feedData(currentNode, bag);
								}
							}
						}
					}
				});
			}

			if (this.boltLevalEmitter != null) {
				windowPO.addEmitter(boltLevalEmitter);
			}

			windowPO.prepare(stormConf, executor,
					((PhysicalOperator) windowPO).getAlias());
		}
	}

	public void setBoltLevalEmitter(Emitter emitter) {
		this.boltLevalEmitter = emitter;
	}

	public void setBoltLevalInputOperators(Map<String, List<String>> operators) {
		this.boltLevalInputMap.clear();
		if (operators == null) {
			return;
		}
		Iterator<String> its = operators.keySet().iterator();
		String a = null;
		while (its.hasNext()) {
			Set<String> set = new HashSet<String>();
			a = its.next();
			set.addAll(operators.get(a));
			this.boltLevalInputMap.put(a, set);
		}
	}

	public Map<String, Set<String>> getBoltLevalInputOperators() {
		return boltLevalInputMap;
	}

	public void setBoltLevalOutputOperators(List<String> operators) {
		if (operators != null) {
			boltLevalOutputOperatorSet.addAll(operators);
		}
	}

	// key:fromAlias(前一级processoroperator的alias) value:toAlias list
	public Map<String, Set<String>> getInputMap() {
		return this.inputMap;
	}

	public void putAllInputMap(Map<String, Set<String>> fromToMap) {
		this.inputMap.putAll(fromToMap);
	}

	// key:fromAlias(当前porcessoroperator中需要发往下一级processoroperator的alias)
	// value:toAlias
	public Map<String, Set<String>> getOutputMap() {
		return this.outputMap;
	}

	public void putAllOutputMap(Map<String, Set<String>> fromToMap) {
		Iterator<String> keys = fromToMap.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			if (outputMap.containsKey(key)) {
				Set<String> values = outputMap.get(key);
				values.addAll(fromToMap.get(key));
			} else {
				outputMap.put(key, fromToMap.get(key));
			}
		}
	}

	public boolean isRoot() {
		return this.isRootProcessorOperator;
	}

	public PhysicalPlan getInnerPlan() {
		return this.innerPlan;
	}

	public void feedData(String fromAlias, DataBag bag) throws Exception {
		if (isWindowProcessor()) {
			((PhysicalOperator) windowPO).attachInputBag(new DataBag[] { bag });
		} else {
			synchronized (lock) {
				try {
					// 第一个节点的情况 ，数据输入为上一级bolt或者tap
					if (isRoot()) {
						attachRoot(this.boltLevalInputMap, fromAlias, bag);
					} else {
						attachRoot(this.getInputMap(), fromAlias, bag);
					}
					executeAndEmit(fromAlias, bag);
				} finally {
					this.innerPlan.detachAllInputBag();
				}
			}
		}
	}

	private void executeAndEmit(String fromAlias, DataBag bag) throws Exception {
		Iterator<String> its = this.startCalculatingPOSet.iterator();
		String a = null;
		while (its.hasNext()) {
			a = its.next();
			Result ret = this.innerPlan.getOperator(a).getNextDataBag();
			if (ret.returnStatus == POStatus.STATUS_OK) {
				emit(a, (DataBag) ret.result);
			} else if (ret.returnStatus == POStatus.STATUS_ERR) {
				LOG.error("Error when process data.Leaf physicaloperator is:"
						+ this.innerPlan.getOperator(a));
			} else if (ret.returnStatus != POStatus.STATUS_NULL) {
				LOG.error("Unexpected result:" + ret
						+ ",Leaf physicaloperator is:"
						+ this.innerPlan.getOperator(a) + ",from alias:"
						+ fromAlias + ",input databag:" + bag);
			}
		}
	}

	// 有数据输入的root节点赋以相应的值，没有数据输入的节点置为空
	private void attachRoot(Map<String, Set<String>> map, String fromAlias,
			DataBag bag) {
		if (isTap) {
			roots.get(0).attachInputBag(bag);
			return;
		}

		Iterator<PhysicalOperator> ito = roots.iterator();
		PhysicalOperator root = null;
		while (ito.hasNext()) {
			root = ito.next();
			if (map.get(fromAlias).contains(root.getAlias())) {
				root.attachInputBag(fromAlias, bag);
			} else {
				root.attachInputBag((DataBag[]) null);
			}
		}
	}

	private void buildStartCalculatingPO() {
		startCalculatingPOSet.clear();

		// 所有的叶子节点均需要出发计算
		for (PhysicalOperator po : this.innerPlan.getLeaves()) {
			startCalculatingPOSet.add(po.getAlias());
		}

		// 发往下一级PorcessorOperator的节点需要出发计算，有可能跟leaf有重复
		Iterator<String> it = getOutputMap().keySet().iterator();
		while (it.hasNext()) {
			startCalculatingPOSet.add(it.next());
		}

		// 直接发往下一级bolt的节点（可能是非叶子节点）需要被触发计算
		if (boltLevalOutputOperatorSet != null
				&& boltLevalOutputOperatorSet.size() > 0) {
			Iterator<PhysicalOperator> ito = this.innerPlan.iterator();
			String a = null;
			while (ito.hasNext()) {
				a = ito.next().getAlias();
				if (boltLevalOutputOperatorSet.contains(a)) {
					startCalculatingPOSet.add(a);
				}
			}
		}
	}

	public void cleanup() {
		if (windowPO != null) {
			windowPO.cleanup();
		}
	}

	private void emit(String currentNode, DataBag bag) throws Exception {
		if (this.boltLevalOutputOperatorSet.contains(currentNode)) {
			this.boltLevalEmitter.emit(currentNode, bag);
		}

		if (this.aliasToNextProcessorOperatorMap.containsKey(currentNode)) {
			Iterator<ProcessorOperator> pos = aliasToNextProcessorOperatorMap
					.get(currentNode).iterator();
			while (pos.hasNext()) {
				ProcessorOperator po = pos.next();
				if (po != null) {
					po.feedData(currentNode, bag);
				}
			}
		}
	}

	private Map<String, Set<ProcessorOperator>> buildOutputAliasToProcessorOperatorMap() {
		Map<String, Set<ProcessorOperator>> map = new HashMap<String, Set<ProcessorOperator>>();
		// outputMap:Map<String, Set<String>> from...to...
		Iterator<String> keys = this.getOutputMap().keySet().iterator();
		String key = null;
		List<ProcessorOperator> successors = null;
		Set<String> outputToAlias = null;
		while (keys.hasNext()) {
			key = keys.next();
			outputToAlias = this.getOutputMap().get(key);
			successors = this.outerPlan.getSuccessors(this);
			if (successors != null && successors.size() > 0) {
				Set<ProcessorOperator> POSet = new HashSet<ProcessorOperator>();
				for (ProcessorOperator p : successors) {
					Iterator<String> its = outputToAlias.iterator();
					while (its.hasNext()) {
						String alias = its.next();
						if (p.innerPlan.getOperator(alias) != null) {
							POSet.add(p);
						}
					}
				}
				map.put(key, POSet);
			}
		}

		return map;
	}

	@Override
	public void visit(ProcessorPlanVistor v) throws VisitorException {
		v.visitProcessorOperator(this);
	}

	@Override
	public boolean supportsMultipleInputs() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean supportsMultipleOutputs() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return "";
	}
}
