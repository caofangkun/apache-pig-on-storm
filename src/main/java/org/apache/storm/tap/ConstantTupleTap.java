package org.apache.storm.tap;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.LoadCaster;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.TapFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.DataUtil;
import org.apache.storm.Utf8TextConverter;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.topologyLayer.Config;
import org.apache.storm.executionengine.topologyLayer.plans.PartitionDecomposer;
import org.apache.storm.executionengine.topologyLayer.plans.TopologyOperatorPlan;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.CastUtils;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

public class ConstantTupleTap extends TapFunc {

  private static final long serialVersionUID = 1L;

  protected ResourceSchema schema;

  private LogicalSchema scriptSchema;

  protected LoadCaster caster;
  //
  protected List<Tuple> Tuples;

  protected int length;

  protected int i = -1;

  public static final Map<String, String[]> data = new HashMap<String, String[]>();

  //

  protected Tuple applySchema(LogicalSchema scriptSchema, Tuple tup)
      throws IOException {
    ResourceSchema schema = null;

    if (caster == null) {
      caster = getLoadCaster();
    }
    if (schema == null) {
      schema = getSchema(scriptSchema);
    }

    if (schema == null) {
      return tup;
    }

    ResourceFieldSchema[] fieldSchemas = schema.getFields();
    int tupleIdx = 0;
    // If some fields have been projected out, the tuple
    // only contains required fields.
    // We walk the requiredColumns array to find required fields,
    // and cast those.
    for (int i = 0; i < Math.min(fieldSchemas.length, tup.size()); i++) {
      Object val = null;
      if (tup.get(tupleIdx) != null) {
        byte[] bytes = ((DataByteArray) tup.get(tupleIdx)).get();
        val = CastUtils.convertToType(caster, bytes, fieldSchemas[i],
            fieldSchemas[i].getType());
        tup.set(tupleIdx, val);
      }
      tupleIdx++;
    }
    // redundant fieldSchemas
    for (int i = tup.size(); i < fieldSchemas.length; i++) {
      tup.append(null);
    }
    return tup;
  }

  protected void putStaticData(Object newValue) throws Exception {
    Field field = this.getClass().getField("data");
    field.setAccessible(true);

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    //
    field.getType().getMethod("put", Object.class, Object.class)
        .invoke(this.getClass().newInstance().data, "data", newValue);

  }

  protected DataBag[] launchPhysicalPlan(PigServer pigServer, String query) {
    try {
      LogicalPlan logicalPlan = pigServer.registerStormQuery(query);
      LogicalPlan lp = pigServer.optimizeLogicalPlan(logicalPlan, true);

      org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan pp = pigServer
          .buildPhysicalPlan(lp);

      TopologyOperatorPlan tp = pigServer.buildTopologyPlan(pp);

      // System.out.println("LP:" + lp.toString());
      // System.out.println("PP:" + pp.toString());
      // System.err.println("TP:" + tp.toString());

      // File lpFile = new File("D:/linux64/test/LP/logical_plan-1111.dot");
      // lp.explain(new PrintStream(lpFile), "dot", true);
      //
      // File ppFile = new File("D:/linux64/test/LP/physical_plan-1111.dot");
      // pp.explain(new PrintStream(ppFile), "dot", true);
      //
      // File tpFile = new File("D:/linux64/test/LP/exec_plan-1111.dot");
      // tp.explain(new PrintStream(tpFile), "dot", true);

      new PartitionDecomposer(pp).visit();

      return executePhysicalPlan(pp);

    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  protected DataBag[] executePhysicalPlan(
      org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan pp)
      throws IOException {
    List<PhysicalOperator> leaves = pp.getLeaves();
    DataBag[] bags = new DataBag[leaves.size()];
    for (int i = 0; i < leaves.size(); i++) {
      PhysicalOperator leaf = leaves.get(i);
      Result res = leaf.getNextDataBag();
      if (res.returnStatus == POStatus.STATUS_OK) {
        DataBag bag = (DataBag) res.result;
        bags[i] = bag;
        continue;
      }

      if (res.returnStatus == POStatus.STATUS_EOP) {
        continue;
      }

      if (res.returnStatus == POStatus.STATUS_NULL) {
        continue;
      }

      if (res.returnStatus == POStatus.STATUS_ERR) {
        String msg;
        if (res.result != null) {
          msg = "Received Error while " + "processing the  plan: " + res.result;
        } else {
          msg = "Received Error while " + "processing the  plan.";
        }
        int errCode = 2090;
        throw new ExecException(msg, errCode, PigException.BUG);
      }
    }

    return bags;
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////////////////

  public void setScriptSchema(LogicalSchema scriptSchema) {
    this.scriptSchema = scriptSchema;
  }

  @Override
  public ResourceSchema getSchema(LogicalSchema scriptSchema) {
    if (scriptSchema == null) {
      return null;
    }
    return new ResourceSchema(scriptSchema);
  }

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
  @Override
  public LoadCaster getLoadCaster() throws IOException {
    return new Utf8TextConverter();
  }

  @Override
  public void open(Config conf) throws IOException {
    if (scriptSchema == null) {
      Tuples = DataUtil.getTuplesFromConstantTupleStringAsByteArray(data
          .get("data"));
    } else {
      Tuples = DataUtil.getTuplesAsByteArray(data.get("data"));
    }
    length = Tuples.size();
  }

  @Override
  public Tuple getNext() throws IOException {
    i++;
    if (i < length) {
      // return Tuples.get(i);
      return applySchema(Tuples.get(i));
    } else {
      return null;
    }
  }

  protected Tuple applySchema(Tuple tup) throws IOException {
    if (caster == null) {
      caster = getLoadCaster();
    }
    if (schema == null) {
      schema = getSchema(scriptSchema);
    }

    if (schema == null) {
      return tup;
    }

    ResourceFieldSchema[] fieldSchemas = schema.getFields();
    int tupleIdx = 0;
    // If some fields have been projected out, the tuple
    // only contains required fields.
    // We walk the requiredColumns array to find required fields,
    // and cast those.
    for (int i = 0; i < Math.min(fieldSchemas.length, tup.size()); i++) {
      Object val = null;
      if (tup.get(tupleIdx) != null) {
        byte[] bytes = ((DataByteArray) tup.get(tupleIdx)).get();
        val = CastUtils.convertToType(caster, bytes, fieldSchemas[i],
            fieldSchemas[i].getType());
        tup.set(tupleIdx, val);
      }
      tupleIdx++;
    }
    // redundant fieldSchemas
    for (int i = tup.size(); i < fieldSchemas.length; i++) {
      tup.append(null);
    }
    return tup;
  }

  @Override
  public void close() throws IOException {
    Tuples.clear();
  }

  @Override
  public void setTxId(long txId) {
  }

  @Override
  public void ack(long txId) {
  }

  @Override
  public Integer getParallelism(Integer parallelismHint) {
    // Since we don't split the file
    return 1;
  }

  public String[] toFields(String fieldList) {
    return fieldList.split(",");
  }

}
