package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Utils;

public class PONative extends PhysicalOperator {

  private static final long serialVersionUID = 1L;

  String nativeMRjar;
  String[] params;

  public PONative(OperatorKey k) {
    super(k);
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitNative(this);
  }

  @Override
  public String name() {
    return getAliasString() + "Native" + "('hadoop jar " + nativeMRjar + " "
        + Utils.getStringFromArray(getParams()) + "')" + " - "
        + mKey.toString();
  }

  public String getNativeMRjar() {
    return nativeMRjar;
  }

  public void setNativeMRjar(String nativeMRjar) {
    this.nativeMRjar = nativeMRjar;
  }

  public String[] getParams() {
    unquotePropertyParams();
    return params;
  }

  /**
   * if there is a argument that starts with "-D", unquote the value part to
   * support use case in PIG-1917
   */
  private void unquotePropertyParams() {
    for (int i = 0; i < params.length; i++) {
      String param = params[i];
      if (param.startsWith("-D")) {
        int equalPos = param.indexOf('=');
        // to unquote, there should be a '=', then at least two quotes
        if (equalPos == -1 || equalPos >= param.length() - 3)
          continue;

        if (checkQuote(equalPos + 1, param, '\'')
            || checkQuote(equalPos + 1, param, '"')) {
          // found quoted value part, remove the quotes
          params[i] = param.substring(0, equalPos + 1)
              + param.substring(equalPos + 2, param.length() - 1);
        }
      }
    }
  }

  private boolean checkQuote(int i, String param, char quote) {
    return param.charAt(i) == quote
        && param.charAt(param.length() - 1) == quote;
  }

  public void setParams(String[] params) {
    this.params = params;
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
    return null;
  }
}
