package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.executionengine.physicalLayer.POStatus;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.Result;
import org.apache.storm.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POGroup extends PhysicalOperator {
  private static final long serialVersionUID = 1L;
  private static final Log log = LogFactory.getLog(POGroup.class);
  protected List<List<PhysicalPlan>> allPlans = new ArrayList<List<PhysicalPlan>>();
  private GroupImp imp = new GroupImp();
  private Type type = Type.GROUP;
  private int outputFieldsCount = 0;

  public enum Type {
    GROUP, INNER, LEFT, RIGHT, FULL
  }

  public POGroup(OperatorKey k) {
    super(k);
  }

  public void setType(Type type) {
    this.type = type;
  }

  public void setFieldsCount(int count) {
    this.outputFieldsCount = count;
  }

  public List<List<PhysicalPlan>> getAllPlans() {
    return imp.getAllPlans();
    // return this.allPlans;
  }

  public void setAllPlans(List<List<PhysicalPlan>> allPlans) {
    imp.setAllPlans(allPlans);
    // this.allPlans = allPlans;
    // for (int i = 0; i < allPlans.size(); i++) {
    // this.results.add(new ResultContainer());
    // }
  }

  // private void resetContainers() {
  // for (ResultContainer rc : this.results) {
  // rc.clear();
  // }
  // }

  public Result processDataBags(DataBag... bags) throws ExecException {
    return imp.processDataBags(bags);
    // if (bags.length != this.allPlans.size()) {
    // throw new ExecException(
    // "Size of input databag does not equal to expressio plans.");
    // }
    //
    // boolean isAllBagNull = true;
    // for (DataBag bag : bags) {
    // if (bag != null && bag.size() > 0) {
    // isAllBagNull = false;
    // break;
    // }
    // }
    // if (isAllBagNull) {
    // Result r = new Result();
    // r.returnStatus = POStatus.STATUS_NULL;
    // return r;
    // }
    //
    // resetContainers();
    //
    // DataBag bag = null;
    // List<PhysicalPlan> plans = null;
    //
    // for (int i = 0; i < bags.length; i++) {
    // bag = bags[i];
    // plans = this.allPlans.get(i);
    // if (bag != null) {
    // Iterator<Tuple> it = bag.iterator();
    // Tuple t = null;
    // while (it.hasNext()) {
    // t = it.next();
    // Object[] keys = new Object[plans.size()];
    // PhysicalOperator leaf = null;
    // PhysicalPlan plan = null;
    // for (int j = 0; j < plans.size(); j++) {
    // plan = plans.get(j);
    // try {
    // plan.attachInput(t);
    // leaf = plan.getLeaves().get(0);
    // Result r = leaf.getNext(leaf.getResultType());
    // if (r.returnStatus != POStatus.STATUS_OK
    // && r.returnStatus != POStatus.STATUS_NULL) {
    // log.info("Not expected result:" + r);
    // r.result = null;
    // return r;
    // }
    // keys[j] = r.result;
    // } finally {
    // leaf.recursiveDetachInput();
    // }
    // }
    // this.results.get(i).addTuple(t, keys);
    // }
    // }
    // }
    // return buildResult();
  }

  // private Result buildResult() {
  // List<Tuple> nullTuples = new ArrayList<Tuple>(this.results.size());
  // for (int i = 0; i < this.results.size(); i++) {
  // ResultContainer rc = this.results.get(i);
  // Map<Object, DataBag> rs = rc.getResult();
  // if (rs != null && rs.containsKey(null)) {
  // DataBag bag = rs.remove(null);
  // if (bag != null && bag.size() > 0) {
  // Tuple t = TupleFactory.getInstance().newTuple();
  // t.append(null);
  // for (int j = 0; j < this.results.size(); j++) {
  // t.append(j == i ? bag : new NonSpillableDataBag());
  // }
  // nullTuples.add(t);
  // }
  // }
  // }
  //
  // List<Tuple> nonNullTuples = new ArrayList<Tuple>();
  //
  // Set<Object> s = new HashSet<Object>();
  // for (ResultContainer rc : this.results) {
  // s.addAll(rc.getResult().keySet());
  // }
  // Iterator<Object> it = s.iterator();
  // while (it.hasNext()) {
  // Object key = it.next();
  // Tuple t = TupleFactory.getInstance().newTuple();
  // t.append(key);
  // for (ResultContainer rc : this.results) {
  // Object value = rc.getResult().remove(key);
  // t.append(value != null ? value : new NonSpillableDataBag());
  // }
  // nonNullTuples.add(t);
  // }
  //
  // nonNullTuples.addAll(nullTuples);
  // NonSpillableDataBag nsdb = new NonSpillableDataBag(nonNullTuples);
  // Result r = new Result();
  // r.returnStatus = POStatus.STATUS_OK;
  // r.result = nsdb;
  // return r;
  // }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    v.visitGroup(this);
  }

  @Override
  public boolean supportsMultipleInputs() {
    return true;
  }

  @Override
  public boolean supportsMultipleOutputs() {
    return true;
  }

  @Override
  public String name() {
    if (this.type == Type.GROUP) {
      return getAliasString() + "Group" + "["
          + DataType.findTypeName(resultType) + "]" + " - " + mKey.toString();
    } else {
      return getAliasString() + "Join" + "["
          + DataType.findTypeName(resultType) + "]" + " - " + mKey.toString();
    }
  }

  @Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
    // TODO
    return null;
  }

  public static class GroupImp implements Serializable {
    private static final long serialVersionUID = 1L;
    protected List<List<PhysicalPlan>> allPlans = new ArrayList<List<PhysicalPlan>>();
    protected List<ResultContainer> results = new ArrayList<ResultContainer>();

    public List<List<PhysicalPlan>> getAllPlans() {
      return this.allPlans;
    }

    public void setAllPlans(List<List<PhysicalPlan>> allPlans) {
      this.allPlans = allPlans;
      for (int i = 0; i < allPlans.size(); i++) {
        this.results.add(new ResultContainer());
      }
    }

    private void resetContainers() {
      for (ResultContainer rc : this.results) {
        rc.clear();
      }
    }

    public Result processDataBags(DataBag... obags) throws ExecException {
      DataBag[] bags = null;
      List<DataBag> noneNullList = new ArrayList<DataBag>();
      if (obags.length > 1 && this.allPlans.size() == 1) {
        for (DataBag bag : obags) {
          if (bag != null) {
            noneNullList.add(bag);
          }
        }
        bags = noneNullList.toArray(new DataBag[noneNullList.size()]);

      } else {
        bags = obags;
      }
      //
      if (bags.length != this.allPlans.size()) {
        throw new ExecException(
            "Size of input databag does not equal to expression plans.");
      }

      boolean isAllBagNull = true;
      for (DataBag bag : bags) {
        if (bag != null && bag.size() > 0) {
          isAllBagNull = false;
          break;
        }
      }
      if (isAllBagNull) {
        Result r = new Result();
        r.returnStatus = POStatus.STATUS_NULL;
        return r;
      }

      resetContainers();

      DataBag bag = null;
      List<PhysicalPlan> plans = null;

      for (int i = 0; i < bags.length; i++) {
        bag = bags[i];
        plans = this.allPlans.get(i);
        if (bag != null) {
          Iterator<Tuple> it = bag.iterator();
          Tuple t = null;
          while (it.hasNext()) {
            t = it.next();
            Object[] keys = new Object[plans.size()];
            PhysicalOperator leaf = null;
            PhysicalPlan plan = null;
            for (int j = 0; j < plans.size(); j++) {
              plan = plans.get(j);
              try {
                plan.attachInput(t);
                leaf = plan.getLeaves().get(0);
                Result r = leaf.getNext(leaf.getResultType());
                if (r.returnStatus != POStatus.STATUS_OK
                    && r.returnStatus != POStatus.STATUS_NULL) {
                  log.info("Not expected result:" + r);
                  r.result = null;
                  return r;
                }
                keys[j] = r.result;
              } finally {
                leaf.recursiveDetachInput();
              }
            }
            this.results.get(i).addTuple(t, keys);
          }
        }
      }
      return buildResult();
    }

    private Result buildResult() {
      List<Tuple> nullTuples = new ArrayList<Tuple>(this.results.size());
      for (int i = 0; i < this.results.size(); i++) {
        ResultContainer rc = this.results.get(i);
        Map<Object, DataBag> rs = rc.getResult();
        if (rs != null && rs.containsKey(null)) {
          DataBag bag = rs.remove(null);
          if (bag != null && bag.size() > 0) {
            Tuple t = TupleFactory.getInstance().newTuple();
            t.append(null);
            for (int j = 0; j < this.results.size(); j++) {
              t.append(j == i ? bag : new NonSpillableDataBag());
            }
            nullTuples.add(t);
          }
        }
      }

      List<Tuple> nonNullTuples = new ArrayList<Tuple>();

      Set<Object> s = new HashSet<Object>();
      for (ResultContainer rc : this.results) {
        s.addAll(rc.getResult().keySet());
      }
      Iterator<Object> it = s.iterator();
      while (it.hasNext()) {
        Object key = it.next();
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(key);
        for (ResultContainer rc : this.results) {
          Object value = rc.getResult().remove(key);
          t.append(value != null ? value : new NonSpillableDataBag());
        }
        nonNullTuples.add(t);
      }

      nonNullTuples.addAll(nullTuples);
      NonSpillableDataBag nsdb = new NonSpillableDataBag(nonNullTuples);
      Result r = new Result();
      r.returnStatus = POStatus.STATUS_OK;
      r.result = nsdb;
      return r;
    }

    private class ResultContainer implements Serializable {
      private static final long serialVersionUID = 1L;
      private Map<Object, DataBag> container = new HashMap<Object, DataBag>();

      public Map<Object, DataBag> getResult() {
        return this.container;
      }

      public void addTuple(Tuple tuple, Object... keys) {
        Object key = null;
        DataBag tuples = null;
        if (keys != null && keys.length > 1) {
          key = TupleFactory.getInstance().newTupleNoCopy(asList(keys));
        } else {
          if (keys != null && keys.length == 1) {
            key = keys[0];
          } else {
            key = null;
          }
        }

        tuples = this.container.get(key);
        if (tuples == null) {
          tuples = new NonSpillableDataBag();
          this.container.put(key, tuples);
        }

        if (tuple != null) {
          tuples.add(tuple);
        }
      }

      private List<Object> asList(Object[] arrays) {
        List<Object> list = new ArrayList<Object>(arrays.length);
        for (Object v : arrays) {
          list.add(v);
        }
        return list;
      }

      public void clear() {
        this.container.clear();
      }
    }
  }
}
