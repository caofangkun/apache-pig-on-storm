package org.apache.storm.tap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.LoadCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.TapFunc;
import org.apache.storm.Utf8TextConverter;
import org.apache.storm.executionengine.topologyLayer.Config;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.CastUtils;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

public class ClasspathResourceTap extends TapFunc {

  private static final long serialVersionUID = 1L;

  private static String DEFAULT_DELIMITER = "\t";

  protected String delimiter;

  protected String location;

  protected BufferedReader reader;

  protected TupleFactory tf;

  protected ResourceSchema schema;

  private LogicalSchema scriptSchema;

  protected LoadCaster caster;

  public ClasspathResourceTap(String location) {
    this(location, DEFAULT_DELIMITER);
  }

  public ClasspathResourceTap(String location, String delimiter) {
    this.location = location;
    this.delimiter = delimiter;
  }

  public void setScriptSchema(LogicalSchema scriptSchema) {
    this.scriptSchema = scriptSchema;
  }

  @Override
  public ResourceSchema getSchema(LogicalSchema scriptSchema) {
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
    tf = TupleFactory.getInstance();
    // TODO /org/apache/pig/test/data/storm/excite-small.log
    reader = new BufferedReader(new InputStreamReader(
        ClasspathResourceTap.class.getResourceAsStream(location)));
  }

  @Override
  public Tuple getNext() throws IOException {
    String line = reader.readLine();
    Tuple result = null;
    if (line != null) {
      String[] record = recordize(line);
      List rec = new ArrayList();
      for (int i = 0; i < record.length; i++) {
        rec.add(new DataByteArray(record[i]));
      }
      result = tf.newTuple(rec);
      return applySchema(result);
    } else {
      return null;
    }

  }

  private Tuple applySchema(Tuple tup) throws IOException {
    if (caster == null) {
      caster = getLoadCaster();
    }
    if (schema == null) {
      schema = getSchema(scriptSchema);
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
    reader.close();
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

  public String[] recordize(String line) {
    return line.split(delimiter);
  }

}
