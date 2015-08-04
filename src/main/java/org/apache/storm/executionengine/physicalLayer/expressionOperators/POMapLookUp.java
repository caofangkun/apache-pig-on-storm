package org.apache.storm.executionengine.physicalLayer.expressionOperators;

import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POMapLookUp extends ExpressionOperator {

  private static final long serialVersionUID = 1L;
  private String key;

  public POMapLookUp(OperatorKey k) {
    super(k);
  }

  public POMapLookUp(OperatorKey k, int rp) {
    super(k, rp);
  }

  public POMapLookUp(OperatorKey k, int rp, String key) {
    super(k, rp);
    this.key = key;
  }

  public void setLookUpKey(String key) {
    this.key = key;
  }

  public String getLookUpKey() {
    return key;
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitMapLookUp(this);

  }

  @Override
  public String name() {
    return "POMapLookUp" + "[" + DataType.findTypeName(resultType) + "]"
        + " - " + mKey.toString();
  }

  @Override
  public boolean supportsMultipleInputs() {
    return false;
  }

  @Override
  public Result processInput() throws ExecException {
    Result res = new Result();
    Map<String, Object> inpValue = null;
    if (input == null && (inputs == null || inputs.size() == 0)) {
      // log.warn("No inputs found. Signaling End of Processing.");
      res.returnStatus = POStatus.STATUS_EOP;
      return res;
    }
    if (!isInputAttached())
      return inputs.get(0).getNextMap();
    else {
      res.result = input;
      res.returnStatus = POStatus.STATUS_OK;
      detachInput();
      return res;
    }
  }

  @SuppressWarnings("unchecked")
  private Result getNext() throws ExecException {
    Result res = processInput();
    if (res.result != null && res.returnStatus == POStatus.STATUS_OK) {
      res.result = ((Map<String, Object>) res.result).get(key);
    }
    return res;
  }

  @Override
  public Result getNextBoolean() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextDataBag() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextDataByteArray() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextDouble() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextFloat() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextInteger() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextLong() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextDateTime() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextMap() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextString() throws ExecException {
    return getNext();
  }

  @Override
  public Result getNextTuple() throws ExecException {
    return getNext();
  }

  @Override
  public POMapLookUp clone() throws CloneNotSupportedException {
    POMapLookUp clone = new POMapLookUp(new OperatorKey(mKey.scope,
        NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)), -1, key);
    clone.cloneHelper(this);
    return clone;
  }

  @Override
  public List<ExpressionOperator> getChildExpressions() {
    return null;
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    return (Tuple) out;
  }
}
