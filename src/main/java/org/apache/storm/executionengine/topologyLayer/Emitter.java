package org.apache.storm.executionengine.topologyLayer;

import java.io.Serializable;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;

public interface Emitter extends Serializable{
  /**
   * @param alias alias of the operator which will emit data.
   * @param bag 
   */
  void emit(String alias,DataBag bag) throws Exception;
}
