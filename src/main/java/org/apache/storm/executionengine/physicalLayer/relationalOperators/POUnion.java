package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POUnion extends PhysicalOperator {
	private static final Log log = LogFactory.getLog(POUnion.class);

	private static final long serialVersionUID = 1L;

	public POUnion(OperatorKey k) {
		this(k, -1, null);
	}

	public POUnion(OperatorKey k, int rp) {
		this(k, rp, null);
	}

	public POUnion(OperatorKey k, List<PhysicalOperator> inp) {
		this(k, -1, inp);
	}

	public POUnion(OperatorKey k, int rp, List<PhysicalOperator> inp) {
		super(k, rp, inp);
	}

	@Override
	public void visit(PhyPlanVisitor v) throws VisitorException {
		v.visitUnion(this);
	}

	@Override
	public String name() {
		return getAliasString() + "Union" + "["
				+ DataType.findTypeName(resultType) + "]" + " - "
				+ mKey.toString();
	}

	@Override
	public boolean supportsMultipleInputs() {
		return true;
	}

	@Override
	public boolean supportsMultipleOutputs() {
		return true;
	}

	@Override
	public Result processDataBags(DataBag... bags) throws ExecException {
		if (bags.length == 0) {
			return new Result(POStatus.STATUS_NULL, null);
		}

		NonSpillableDataBag b = new NonSpillableDataBag();

		for (DataBag bag : bags) {
			if (bag != null && bag.size() > 0) {
				b.addAll(bag);
			}
		}

		if (b.size() == 0) {
			return new Result(POStatus.STATUS_NULL, null);
		} else {
			log.debug("Union output:" + alias + ":" + b);
			return new Result(POStatus.STATUS_OK, b);
		}
	}

	@Override
	public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
		return null;
	}
}
