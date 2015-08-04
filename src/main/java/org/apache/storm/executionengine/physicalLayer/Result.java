package org.apache.storm.executionengine.physicalLayer;

import java.io.Serializable;

public class Result implements Serializable {
  /**
     * 
     */
  private static final long serialVersionUID = 1L;

  /*
   * When returnStatus is set to POStatus.STATUS_ERR Operators can choose to use
   * the result field to put a meaning error message which will be printed out
   * in the final message shown to the user
   */
  public byte returnStatus;

  public Object result;

  public Result() {
    returnStatus = POStatus.STATUS_ERR;
    result = null;
  }

  public Result(byte returnStatus, Object result) {
    this.returnStatus = returnStatus;
    this.result = result;
  }

  public String toString() {
    return (result != null) ? result.toString() : "NULL";
  }

}
