package org.apache.storm.executionengine.topologyLayer;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.DotPOPrinter;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.DotPlanDumper;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanVisitor;

public class ProcessPlanDotPrinter
		extends
		DotPlanDumper<ProcessorOperator, ProcessorOperatorPlan, ProcessPlanDotPrinter.InnerOperator, ProcessPlanDotPrinter.InnerPlan> {

	static int counter = 0;
	boolean isVerboseNesting = true;

	public ProcessPlanDotPrinter(ProcessorOperatorPlan plan, PrintStream ps) {
		this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
				new HashSet<Operator>());
	}

	private ProcessPlanDotPrinter(ProcessorOperatorPlan plan, PrintStream ps,
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
	protected String getName(ProcessorOperator op) {
		StringBuffer mStream = new StringBuffer();
		if (op.getInputMap().size() > 0) {
			mStream.append(" Input:");
			for (Map.Entry<String, Set<String>> entry : op.getInputMap()
					.entrySet()) {
				StringBuilder sb = new StringBuilder(entry.getKey() + "\n");
				sb.append(entry.getValue());
				mStream.append(sb.toString());
			}

		}
		if (op.getOutputMap().size() > 0) {
			mStream.append(" Output:");
			for (Map.Entry<String, Set<String>> entry : op.getOutputMap()
					.entrySet()) {
				StringBuilder sb = new StringBuilder(entry.getKey() + "\n");
				sb.append(entry.getValue());
				mStream.append(sb.toString());
			}

		}
		String name = "ProcessorOperator " + mStream.toString();
		return name;
	}

	@Override
	protected Collection<InnerPlan> getNestedPlans(ProcessorOperator op) {
		Collection<InnerPlan> plans = new LinkedList<InnerPlan>();
		plans.add(new InnerPlan(op.getInnerPlan()));
		return plans;
	}

	@Override
	protected String[] getAttributes(ProcessorOperator op) {
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
			InnerOperator map = new InnerOperator(mapPlan, "ProcessorOperator");
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
