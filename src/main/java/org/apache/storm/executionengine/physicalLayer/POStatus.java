package org.apache.storm.executionengine.physicalLayer;

public class POStatus {

  public static final byte STATUS_OK = 0;

  public static final byte STATUS_NULL = 1;

  public static final byte STATUS_ERR = 2;

  public static final byte STATUS_EOP = 3; // end of processing

  // This is currently only used in communications
  // between ExecutableManager and POStream
  public static final byte STATUS_EOS = 4; // end of Streaming output (i.e.
  // output from streaming binary)

  // successfully processing of a batch, used by accumulative UDFs
  // this is used for accumulative UDFs
  public static final byte STATUS_BATCH_OK = 5;

  // this signals that an accumulative UDF has already finished
  public static final byte STATUS_EARLY_TERMINATION = 6;
}
