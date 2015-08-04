package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.DumpFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.SortInfo;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.builtin.SimpleTextDumper;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

public class PODump extends PhysicalOperator {

  private static final Log log = LogFactory.getLog(PODump.class);

  private static final long serialVersionUID = 1L;

  private static Result empty = new Result(POStatus.STATUS_NULL, null);

  transient private DumpFunc dumper;

  // private FileSpec sFile;
  FuncSpec fs;

  private Schema schema;

  // flag to distinguish user stores from MRCompiler stores.
  private boolean isTmpStore;

  // flag to distinguish single store from multiquery store.
  private boolean isMultiStore;

  // flag to indicate if the custom counter should be disabled.
  private boolean disableCounter = false;

  // the index of multiquery store to track counters
  private int index;

  // If we know how to reload the store, here's how. The lFile
  // FileSpec is set in PigServer.postProcess. It can be used to
  // reload this store, if the optimizer has the need.
  private FileSpec lFile;

  // if the predecessor of store is Sort (order by)
  // then sortInfo will have information of the sort
  // column names and the asc/dsc info
  private SortInfo sortInfo;

  private String signature;

  public PODump(OperatorKey k) {
    this(k, -1, null);
  }

  public PODump(OperatorKey k, int rp) {
    this(k, rp, null);
  }

  public PODump(OperatorKey k, int rp, List<PhysicalOperator> inp) {
    super(k, rp, inp);
  }

  /**
   * Set up the dumper
   * 
   * @throws IOException
   */
  public void setUp() throws IOException {
    try {
      FuncSpec funcSpec = fs == null ? new FuncSpec(
          SimpleTextDumper.class.getName()) : fs;
      dumper = (DumpFunc) PigContext.instantiateFuncFromSpec(funcSpec);
      dumper.prepareToWrite();// TODO
    } catch (IOException ioe) {
      int errCode = 2081;
      String msg = "Unable to setup the dump function.";
      throw new ExecException(msg, errCode, PigException.BUG, ioe);
    }
  }

  /**
   * Called at the end of processing for clean up.
   * 
   * @throws IOException
   */
  public void tearDown() throws IOException {
    dumper.cleanupOnSuccess();
  }

  @Override
  public Result getNextTuple() throws ExecException {
    Result res = processInput();
    try {
      switch (res.returnStatus) {
      case POStatus.STATUS_OK:

        if (illustrator == null) {
          dumper.putNext((Tuple) res.result);
        } else
          illustratorMarkup(res.result, res.result, 0);
        res = empty;
        break;
      case POStatus.STATUS_EOP:
        break;
      case POStatus.STATUS_ERR:
      case POStatus.STATUS_NULL:
      default:
        break;
      }
    } catch (IOException ioe) {
      int errCode = 2135;
      String msg = "Received error from dump function." + ioe.getMessage();
      throw new ExecException(msg, errCode, ioe);
    }
    return res;
  }

  public Result processDataBags(DataBag... bags) throws ExecException {
    DataBag bag = bags[0];
    Result r = new Result();
    if (bag == null || bag.size() == 0) {
      r.returnStatus = POStatus.STATUS_NULL;
      return r;
    }

    Iterator<Tuple> it = bag.iterator();
    //
    Result res = null;
    Tuple inp = null;
    try {
      setUp();
      while (it.hasNext()) {
        inp = it.next();
        //
        dumper.putNext(inp);
        log.debug("Dump output:" + alias + ":" + inp);
        res = empty;
        //
      }
      tearDown();
    } catch (IOException ioe) {
      int errCode = 2135;
      String msg = "Received error from dump function." + ioe.getMessage();
      throw new ExecException(msg, errCode, ioe);
    }
    return res;
  }

  @Override
  public String name() {
    return (fs != null) ? getAliasString() + "Dump" + "(" + fs.toString() + ")"
        + " - " + mKey.toString() : getAliasString() + "Dump" + "("
        + "DummyFil:DummyLdr" + ")" + " - " + mKey.toString();
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
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitDump(this);
  }

  public FuncSpec getFuncSpec() {
    return fs;
  }

  public void setFuncSpec(FuncSpec fs) {
    this.fs = fs;
    dumper = null;
  }

  public void setInputSpec(FileSpec lFile) {
    this.lFile = lFile;
  }

  public FileSpec getInputSpec() {
    return lFile;
  }

  public void setIsTmpStore(boolean tmp) {
    isTmpStore = tmp;
  }

  public boolean isTmpStore() {
    return isTmpStore;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }

  public DumpFunc getDumpFunc() {
    if (dumper == null) {
      dumper = (DumpFunc) PigContext.instantiateFuncFromSpec(fs);
      dumper.setDumpFuncUDFContextSignature(signature);
    }
    return dumper;
  }

  /**
   * @param sortInfo
   *          the sortInfo to set
   */
  public void setSortInfo(SortInfo sortInfo) {
    this.sortInfo = sortInfo;
  }

  /**
   * @return the sortInfo
   */
  public SortInfo getSortInfo() {
    return sortInfo;
  }

  public String getSignature() {
    return signature;
  }

  public void setSignature(String signature) {
    this.signature = signature;
  }

  public void setMultiStore(boolean isMultiStore) {
    this.isMultiStore = isMultiStore;
  }

  public boolean isMultiStore() {
    return isMultiStore;
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    if (illustrator != null) {
      ExampleTuple tIn = (ExampleTuple) in;
      LineageTracer lineage = illustrator.getLineage();
      lineage.insert(tIn);
      if (!isTmpStore)
        illustrator.getEquivalenceClasses().get(eqClassIndex).add(tIn);
      illustrator.addData((Tuple) out);
    }
    return (Tuple) out;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  public void setDisableCounter(boolean disableCounter) {
    this.disableCounter = disableCounter;
  }

  public boolean disableCounter() {
    return disableCounter;
  }

  public void setDumpFunc(DumpFunc dumpFunc) {
    this.dumper = dumpFunc;
  }
}
