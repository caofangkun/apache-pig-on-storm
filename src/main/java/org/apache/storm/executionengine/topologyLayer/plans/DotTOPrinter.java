package org.apache.storm.executionengine.topologyLayer.plans;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.DotPOPrinter;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.storm.executionengine.topologyLayer.TopologyOperator;
import org.apache.pig.impl.plan.DotPlanDumper;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanVisitor;

/**
 * This class can print an Topology Operator plan in the DOT format. It uses
 * clusters to illustrate nesting. If "verbose" is off, it will skip any nesting
 * in the associated physical plans.
 */
public class DotTOPrinter
		extends
		DotPlanDumper<TopologyOperator, TopologyOperatorPlan, DotTOPrinter.InnerOperator, DotTOPrinter.InnerPlan> {

	static int counter = 0;
	boolean isVerboseNesting = true;

	public DotTOPrinter(TopologyOperatorPlan plan, PrintStream ps) {
		this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
				new HashSet<Operator>());
	}

	private DotTOPrinter(TopologyOperatorPlan plan, PrintStream ps,
			boolean isSubGraph, Set<Operator> subgraphs,
			Set<Operator> multiInputSubgraphs,
			Set<Operator> multiOutputSubgraphs) {
		super(plan, ps, isSubGraph, subgraphs, multiInputSubgraphs,
				multiOutputSubgraphs);
	}

	@Override
	public void setVerbose(boolean verbose) {
		// leave the parents verbose set to true
		isVerboseNesting = verbose;
	}

	@Override
	protected DotPlanDumper makeDumper(InnerPlan plan, PrintStream ps) {
		return new InnerPrinter(plan, ps, mSubgraphs, mMultiInputSubgraphs,
				mMultiOutputSubgraphs);
	}

	@Override
	protected String getName(TopologyOperator op) {
		String name = null;
		if (op.hasTap()) {
			StringBuffer sb = new StringBuffer();
			List<POPartition> outputPartitions = op.getOutputPartitions();
			for (POPartition out : outputPartitions) {
				sb.append(out.getAlias());
				sb.append(",");
			}
			name = "Spout output:" + sb.toString();
		} else {
			StringBuffer input = new StringBuffer();
			List<POPartition> inputPartitions = op.getInputPartitions();
			for (POPartition in : inputPartitions) {
				input.append(in.getAlias());
				input.append(",");
			}
			//
			StringBuffer output = new StringBuffer();
			List<POPartition> outputPartitions = op.getOutputPartitions();
			for (POPartition out : outputPartitions) {
				output.append(out.getAlias());
				output.append(",");
			}
			name = "Bolt input:" + input.toString() + " output:"
					+ output.toString();
		}

		if (op.getRequestedParallelism() != -1) {
			name += " Parallelism: " + op.getRequestedParallelism();
		} else {
			name += " Parallelism: " + 1;
		}

		return name;
	}

	@Override
	protected Collection<InnerPlan> getNestedPlans(TopologyOperator op) {
		Collection<InnerPlan> plans = new LinkedList<InnerPlan>();
		plans.add(new InnerPlan(op.topoPlan));
		return plans;
	}

	@Override
	protected String[] getAttributes(TopologyOperator op) {
		String[] attributes = new String[3];
		attributes[0] = "label=\"" + getName(op) + "\"";
		attributes[1] = "style=\"filled\"";
		attributes[2] = "fillcolor=\"#EEEEEE\"";
		return attributes;
	}

	/**
	 * Helper class to represent the relationship of map, reduce and combine
	 * phases in an MR operator.
	 */
	public static class InnerOperator extends Operator<PlanVisitor> {

		private static final long serialVersionUID = 1L;
		String name;
		PhysicalPlan plan;
		int code;

		public InnerOperator(PhysicalPlan plan, String name) {
			super(new OperatorKey());
			this.name = name;
			this.plan = plan;
			this.code = counter++;
		}

		@Override
		public void visit(PlanVisitor v) {
		}

		@Override
		public boolean supportsMultipleInputs() {
			return false;
		}

		@Override
		public boolean supportsMultipleOutputs() {
			return false;
		}

		@Override
		public String name() {
			return name;
		}

		public PhysicalPlan getPlan() {
			return plan;
		}

		@Override
		public int hashCode() {
			return code;
		}
	}

	/**
	 * Helper class to represent the relationship of map, reduce and combine
	 * phases in an MR operator. Each MR operator will have an inner plan of map
	 * -> (combine)? -> (reduce)? inner operators. The inner operators contain
	 * the physical plan of the execution phase.
	 */
	public static class InnerPlan extends OperatorPlan<InnerOperator> {

		private static final long serialVersionUID = 1L;

		public InnerPlan(PhysicalPlan mapPlan) {
			InnerOperator map = new InnerOperator(mapPlan, "Topology");
			this.add(map);
		}
	}

	/**
	 * Helper class to represent the relationship of map, reduce and combine
	 * phases in an MR operator.
	 */
	private class InnerPrinter
			extends
			DotPlanDumper<InnerOperator, InnerPlan, PhysicalOperator, PhysicalPlan> {

		public InnerPrinter(InnerPlan plan, PrintStream ps,
				Set<Operator> subgraphs, Set<Operator> multiInputSubgraphs,
				Set<Operator> multiOutputSubgraphs) {
			super(plan, ps, true, subgraphs, multiInputSubgraphs,
					multiOutputSubgraphs);
		}

		@Override
		protected String[] getAttributes(InnerOperator op) {
			String[] attributes = new String[3];
			attributes[0] = "label=\"" + super.getName(op) + "\"";
			attributes[1] = "style=\"filled\"";
			attributes[2] = "fillcolor=\"white\"";
			return attributes;
		}

		@Override
		protected Collection<PhysicalPlan> getNestedPlans(InnerOperator op) {
			Collection<PhysicalPlan> l = new LinkedList<PhysicalPlan>();
			l.add(op.getPlan());
			return l;
		}

		@Override
		protected DotPOPrinter makeDumper(PhysicalPlan plan, PrintStream ps) {
			DotPOPrinter printer = new DotPOPrinter(plan, ps, true, mSubgraphs,
					mMultiInputSubgraphs, mMultiOutputSubgraphs);
			printer.setVerbose(isVerboseNesting);
			return printer;
		}
	}
}
