package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * This operator is a variation of PODistinct, the input to this operator must
 * be sorted already.
 * 
 */
public class POSortedDistinct extends PODistinct {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;
  private transient Tuple lastTuple;

  public POSortedDistinct(OperatorKey k, int rp, List<PhysicalOperator> inp) {
    super(k, rp, inp);
  }

  public POSortedDistinct(OperatorKey k, int rp) {
    super(k, rp);
  }

  public POSortedDistinct(OperatorKey k, List<PhysicalOperator> inp) {
    super(k, inp);
  }

  public POSortedDistinct(OperatorKey k) {
    super(k);
  }

  public Result getNextTuple() throws ExecException {
    while (true) {
      Result in = processInput();
      if (in.returnStatus == POStatus.STATUS_NULL) {
        continue;
      }

      if (in.returnStatus == POStatus.STATUS_OK) {
        if (lastTuple == null || !lastTuple.equals(in.result)) {
          lastTuple = (Tuple) in.result;
          return in;
        } else {
          continue;
        }
      }

      if (in.returnStatus == POStatus.STATUS_EOP) {
        if (!isAccumulative() || !isAccumStarted()) {
          lastTuple = null;
        }
        return in;
      }

      // if there is an error, just return
      return in;
    }
  }

  @Override
  public String name() {
    return getAliasString() + "POSortedDistinct" + "["
        + DataType.findTypeName(resultType) + "]" + " - " + mKey.toString();
  }
}
