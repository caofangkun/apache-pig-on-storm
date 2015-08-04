package org.apache.storm;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang.time.DateUtils;
import org.apache.pig.PigServer;
import org.apache.storm.executionengine.physicalLayer.compiler.LogicalRelationalOperatorCompiler;
import org.apache.storm.executionengine.physicalLayer.relationalOperators.POPartition;
import org.apache.storm.executionengine.topologyLayer.Config;
import org.apache.storm.executionengine.topologyLayer.StormLauncher;
import org.apache.storm.executionengine.topologyLayer.TopologyCompiler;
import org.apache.storm.executionengine.topologyLayer.TopologyOperator;
import org.apache.storm.executionengine.topologyLayer.plans.TopologyOperatorPlan;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import com.google.common.base.Splitter;

public class Utils {
  public static Tuple asTuple(Object... fields) {
    List<Object> list = new ArrayList<Object>(fields.length);
    for (Object field : fields) {
      list.add(field);
    }
    return TupleFactory.getInstance().newTuple(list);
  }

  public static DataBag asBag(Object... tuples) {

    List<Tuple> tupleList = new ArrayList<Tuple>(tuples.length);
    for (Object t : tuples) {
      tupleList.add(asTuple(t));
    }
    return asBag(tupleList);
  }

  public static DataBag asBag(List<Tuple> tuples) {
    NonSpillableDataBag nsdb = new NonSpillableDataBag(tuples);
    return nsdb;
  }

  public static DataBag asBag(Tuple... tuples) {
    NonSpillableDataBag nsdb = new NonSpillableDataBag();
    for (Tuple t : tuples) {
      nsdb.add(t);
    }
    return nsdb;
  }

  public static DataBag asOneTupleBag(Object... fields) {
    NonSpillableDataBag nsdb = new NonSpillableDataBag();
    nsdb.add(asTuple(fields));
    return nsdb;
  }

  public static void launchStorm(Map customConf, String query) throws Exception {
    PigServer pigServer = new PigServer("local");
    PigContext pc = pigServer.getPigContext();
    /**
     * 
     */
    LogicalPlan logicalPlan = pigServer.registerStormQuery(query);
    LogicalPlan lp = pigServer.optimizeLogicalPlan(logicalPlan, true);

    org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan pp = pigServer
        .buildPhysicalPlan(lp);

    TopologyOperatorPlan top = pigServer.buildTopologyPlan(pp);
    /**
     * 
     */

    // TopologyOperatorPlan top = getTopologyOperatorPlanByPigServer(pigServer,
    // query);

    System.out.println("import list:" + pc.getPackageImportList());
    System.out.println(top);
    StormLauncher sl = new StormLauncher();
    sl.launchStorm(top, pc, customConf);
  }

  public static void launchStormOnYarn(Map customConf, String query)
      throws Exception {
    PigServer pigServer = new PigServer("local");
    PigContext pc = pigServer.getPigContext();

    LogicalPlan logicalPlan = pigServer.registerStormQuery(query);
    LogicalPlan lp = pigServer.optimizeLogicalPlan(logicalPlan, true);

    org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan pp = pigServer
        .buildPhysicalPlan(lp);

    TopologyOperatorPlan top = pigServer.buildTopologyPlan(pp);

    StormLauncher sl = new StormLauncher();
    sl.launchStormOnYarn(top, pc, customConf);
  }

  public static String getBoltID(TopologyOperator to) {
    List<String> aliases = new ArrayList<String>();
    for (POPartition po : to.getInputPartitions()) {
      aliases.add(po.getAlias());
    }
    return getBoltID(aliases);
  }

  public static String getBoltID(String... inputAliases) {
    List<String> aliases = new ArrayList<String>();
    for (String alias : inputAliases) {
      aliases.add(alias);
    }
    return getBoltID(aliases);
  }

  public static String getBoltID(Collection<String> inputAliases) {
    TreeSet<String> set = new TreeSet<String>(inputAliases);
    String boltID = "Bolt(";
    Iterator<String> it = set.iterator();
    while (it.hasNext()) {
      boltID += it.next();
      if (it.hasNext()) {
        boltID += "-";
      }
    }
    return boltID += ")";
  }

  public static TopologyOperatorPlan getTopologyOperatorPlanByPigServer(
      PigServer ps, String query) throws Exception {
    LogicalPlan lp = ps.getLogicalPlan(query);

    // translate new logical plan to physical plan
    LogicalRelationalOperatorCompiler pCompiler = new LogicalRelationalOperatorCompiler(
        lp);
    pCompiler.setPigContext(ps.getPigContext());
    pCompiler.visit();
    org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan pp = pCompiler
        .getPhysicalPlan();

    TopologyCompiler compiler = new TopologyCompiler(pp, ps.getPigContext());
    compiler.compile();
    TopologyOperatorPlan plan = compiler.getTopologyOperatorPlan();
    return plan;
  }

  public static String[] split(String content, String separator) {
    Iterable<String> it = Splitter.on(separator).split(content);

    List<String> list = new ArrayList<String>();
    for (String v : it) {
      list.add(v.trim());
    }

    return list.toArray(new String[list.size()]);
  }

  public static int timestampAsInt(String timestamp, String... format)
      throws ParseException {
    return (int) (DateUtils.parseDate(timestamp, format).getTime() / 1000);
  }

  public static String get(Config conf, String key, String default_value) {
    Object o = conf.getValue(key);
    if (o != null) {
      return o.toString();
    } else {
      return default_value;
    }
  }

  public static short getShort(Config conf, String key, short default_value) {
    Object o = conf.getValue(key);
    if (o != null) {
      return Short.parseShort(o.toString());
    } else {
      return default_value;
    }
  }

  public static int getInt(Config conf, String key, int default_value) {
    Object o = conf.getValue(key);
    if (o != null) {
      return Integer.parseInt(o.toString());
    } else {
      return default_value;
    }
  }

  public static int getInt(String key, int default_value) {
    try {
      return Integer.parseInt(key);
    } catch (NumberFormatException e) {
      return default_value;
    }
  }

  public static boolean getBoolean(Config conf, String key,
      boolean default_value) {
    Object o = conf.getValue(key);
    if (o != null) {
      if (o.toString().equalsIgnoreCase("true"))
        return true;
      else
        return false;
    } else {
      return default_value;
    }
  }

  public static long getLong(Config conf, String key, long default_value) {
    Object o = conf.getValue(key);
    if (o != null) {
      return Long.parseLong(o.toString());
    } else {
      return default_value;
    }
  }

  public static float getFloat(Config conf, String key, float default_value) {
    Object o = conf.getValue(key);
    if (o != null) {
      try {
        return Float.parseFloat(o.toString());
      } catch (Exception e) {
        return default_value;
      }
    } else {
      return default_value;
    }
  }

  public static String get(Map conf, String key, String default_value) {
    Object o = conf.get(key);
    if (o != null) {
      return o.toString();
    } else {
      return default_value;
    }
  }

  public static short getShort(Map conf, String key, short default_value) {
    Object o = conf.get(key);
    if (o != null) {
      return Short.parseShort(o.toString());
    } else {
      return default_value;
    }
  }

  public static int getInt(Map conf, String key, int default_value) {
    Object o = conf.get(key);
    if (o != null) {
      return Integer.parseInt(o.toString());
    } else {
      return default_value;
    }
  }

  public static boolean getBoolean(Map conf, String key, boolean default_value) {
    Object o = conf.get(key);
    if (o != null) {
      if (o.toString().equalsIgnoreCase("true"))
        return true;
      else
        return false;
    } else {
      return default_value;
    }
  }

  public static long getLong(Map conf, String key, long default_value) {
    Object o = conf.get(key);
    if (o != null) {
      return Long.parseLong(o.toString());
    } else {
      return default_value;
    }
  }

  public static float getFloat(Map conf, String key, float default_value) {
    Object o = conf.get(key);
    if (o != null) {
      try {
        return Float.parseFloat(o.toString());
      } catch (Exception e) {
        return default_value;
      }
    } else {
      return default_value;
    }
  }

}
