package org.apache.storm;

import java.io.IOException;
import java.io.Serializable;

import org.apache.storm.executionengine.topologyLayer.Config;
import org.apache.pig.LoadCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.Tuple;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

public abstract class TapFunc implements Serializable {

  private static final long serialVersionUID = 7695608098118553627L;

  public void setUDFContextSignature(String signature) {
    // default implementation is a no-op
  }

  public abstract void setScriptSchema(LogicalSchema scriptSchema);

  /**
   * Get a schema for the data to be loaded.
   * 
   * @return schema for the data to be loaded. This schema should represent all
   *         tuples of the returned data. If the schema is unknown or it is not
   *         possible to return a schema that represents all returned data, then
   *         null should be returned.
   * 
   * @throws IOException
   *           if an exception occurs while determining the schema
   */
  public abstract ResourceSchema getSchema(LogicalSchema scriptSchema)
      throws IOException;

  /**
   * This will be called on the front end during planning and not on the back
   * end during execution.
   * 
   * @return the {@link LoadCaster} associated with this loader. Returning null
   *         indicates that casts from byte array are not supported for this
   *         loader. construction
   * 
   * @throws IOException
   *           if there is an exception during LoadCaster
   */
  public LoadCaster getLoadCaster() throws IOException {
    return new Utf8StorageConverter();
  }

  public abstract void open(Config conf) throws IOException;

  /**
   * Retrieves the next tuple to be processed. Implementations should NOT reuse
   * tuple objects (or inner member objects) they return across calls and should
   * return a different tuple object in each call.
   * 
   * @return the next tuple to be processed or null if there are no more tuples
   *         to be processed.
   * 
   * @throws IOException
   *           if there is an exception while retrieving the next tuple
   */
  public abstract Tuple getNext() throws IOException;

  public abstract void close() throws IOException;

  // For transactionality
  public abstract void setTxId(long txId);

  public abstract void ack(long txId);

  // For parallelism
  public Integer getParallelism(Integer parallelismHint) {
    return parallelismHint;
  }

}
