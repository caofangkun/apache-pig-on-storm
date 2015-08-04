package org.apache.storm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.QueryParserDriver;
import org.joda.time.DateTime;

public class DataUtil {

  private static final int BUFFER_SIZE = 1024;

  private static final String UTF8 = "UTF-8";

  private static ByteArrayOutputStream mOut = new ByteArrayOutputStream(
      BUFFER_SIZE);

  public static LogicalSchema parseSchema(String pigSchemaAsString)
      throws ParserException {
    QueryParserDriver queryParser = new QueryParserDriver(new PigContext(),
        "util", new HashMap<String, String>());
    return queryParser.parseSchema(pigSchemaAsString);
  }

  public static Object getPigConstant(String pigConstantAsString)
      throws ParserException {
    QueryParserDriver queryParser = new QueryParserDriver(new PigContext(),
        "util", new HashMap<String, String>());
    return queryParser.parseConstant(pigConstantAsString);
  }

  /**
   * Parse list of strings in to list of tuples, convert quoted strings into
   * 
   * @param tupleConstants
   * @return
   * @throws ParserException
   */
  private static List<Tuple> getTuplesFromConstantTupleStrings(
      String[] tupleConstants) throws ParserException {
    List<Tuple> result = new ArrayList<Tuple>(tupleConstants.length);
    for (int i = 0; i < tupleConstants.length; i++) {
      result.add((Tuple) getPigConstant(tupleConstants[i]));
    }
    return result;
  }

  /**
   * Parse list of strings in to list of tuples, convert quoted strings into
   * DataByteArray
   * 
   * @param tupleConstants
   * @return
   * @throws IOException
   */
  public static List<Tuple> getTuplesFromConstantTupleStringAsByteArray(
      String[] tupleConstants) throws IOException {
    List<Tuple> tuples = getTuplesFromConstantTupleStrings(tupleConstants);
    for (Tuple t : tuples) {
      convertStringToDataByteArray(t);
    }
    return tuples;
  }

  public static List<Tuple> getTuplesAsByteArray(String[] tupleConstants)
      throws IOException {
    List<Tuple> tuples = getTuplesFromConstantTupleStrings(tupleConstants);
    for (Tuple t : tuples) {
      convertFieldsToDataByteArray(t);
    }
    return tuples;
  }

  /**
   * Convert String objects in argument t to DataByteArray objects
   * 
   * @param t
   * @throws ExecException
   */
  public static void convertStringToDataByteArray(Tuple t) throws ExecException {
    if (t == null)
      return;
    for (int i = 0; i < t.size(); i++) {
      Object col = t.get(i);
      if (col == null)
        continue;
      if (col instanceof String) {
        DataByteArray dba = (col == null) ? null : new DataByteArray(
            (String) col);
        t.set(i, dba);
      } else if (col instanceof Tuple) {
        convertStringToDataByteArray((Tuple) col);
      } else if (col instanceof DataBag) {
        Iterator<Tuple> it = ((DataBag) col).iterator();
        while (it.hasNext()) {
          convertStringToDataByteArray((Tuple) it.next());
        }
      }

    }
  }

  /**
   * Convert String objects in argument t to DataByteArray objects
   * 
   * @param t
   * @throws IOException
   */
  private static void convertFieldsToDataByteArray(Tuple t) throws IOException {
    if (t == null)
      return;
    for (int i = 0; i < t.size(); i++) {
      Object field = t.get(i);
      // mOut
      putField(field);
      DataByteArray dba = new DataByteArray(mOut.toByteArray());
      mOut.reset(); // mOut
      t.set(i, dba);
    }
  }

  private static void putField(Object field) throws IOException {
    // string constants for each delimiter
    String tupleBeginDelim = "(";
    String tupleEndDelim = ")";
    String bagBeginDelim = "{";
    String bagEndDelim = "}";
    String mapBeginDelim = "[";
    String mapEndDelim = "]";
    String fieldDelim = ",";
    String mapKeyValueDelim = "#";

    switch (DataType.findType(field)) {
    case DataType.NULL:
      break; // just leave it empty
    case DataType.BOOLEAN:
      mOut.write(((Boolean) field).toString().getBytes());
      break;
    case DataType.INTEGER:
      mOut.write(((Integer) field).toString().getBytes());
      break;
    case DataType.LONG:
      mOut.write(((Long) field).toString().getBytes());
      break;
    case DataType.FLOAT:
      mOut.write(((Float) field).toString().getBytes());
      break;
    case DataType.DOUBLE:
      mOut.write(((Double) field).toString().getBytes());
      break;
    case DataType.DATETIME:
      mOut.write(((DateTime) field).toString().getBytes());
      break;
    case DataType.BYTEARRAY: {
      byte[] b = ((DataByteArray) field).get();
      mOut.write(b, 0, b.length);
      break;
    }

    case DataType.CHARARRAY:
      // oddly enough, writeBytes writes a string
      mOut.write(((String) field).getBytes(UTF8));
      break;

    case DataType.MAP:
      boolean mapHasNext = false;
      Map<String, Object> m = (Map<String, Object>) field;
      mOut.write(mapBeginDelim.getBytes(UTF8));
      for (Map.Entry<String, Object> e : m.entrySet()) {
        if (mapHasNext) {
          mOut.write(fieldDelim.getBytes(UTF8));
        } else {
          mapHasNext = true;
          putField(e.getKey());
          mOut.write(mapKeyValueDelim.getBytes(UTF8));
          putField(e.getValue());
        }
      }
      mOut.write(mapEndDelim.getBytes(UTF8));
      break;

    case DataType.TUPLE:
      boolean tupleHasNext = false;
      Tuple t = (Tuple) field;
      mOut.write(tupleBeginDelim.getBytes(UTF8));
      for (int i = 0; i < t.size(); ++i) {
        if (tupleHasNext) {
          mOut.write(fieldDelim.getBytes(UTF8));
        } else {
          tupleHasNext = true;
        }
        try {
          putField(t.get(i));
        } catch (ExecException ee) {
          throw ee;
        }
      }
      mOut.write(tupleEndDelim.getBytes(UTF8));
      break;
    case DataType.BAG:
      boolean bagHasNext = false;
      mOut.write(bagBeginDelim.getBytes(UTF8));
      Iterator<Tuple> tupleIter = ((DataBag) field).iterator();
      while (tupleIter.hasNext()) {
        if (bagHasNext) {
          mOut.write(fieldDelim.getBytes(UTF8));
        } else {
          bagHasNext = true;
        }
        putField((Object) tupleIter.next());
      }
      mOut.write(bagEndDelim.getBytes(UTF8));
      break;
    default: {
      int errCode = 2108;
      String msg = "Could not determine data type of field: " + field;
      throw new ExecException(msg, errCode, PigException.BUG);
    }
    }
  }

}
