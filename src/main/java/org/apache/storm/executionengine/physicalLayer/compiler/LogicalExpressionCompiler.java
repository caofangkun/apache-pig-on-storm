package org.apache.storm.executionengine.physicalLayer.compiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.storm.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.storm.executionengine.physicalLayer.PhysicalOperator;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.Add;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.BinaryComparisonOperator;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.BinaryExpressionOperator;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.Divide;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.GTOrEqualToExpr;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.GreaterThanExpr;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.LTOrEqualToExpr;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.LessThanExpr;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.Mod;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.Multiply;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.NotEqualToExpr;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POAnd;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POIsNull;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POMapLookUp;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.PONegative;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.PONot;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POOr;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.PORegexp;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.PORelationToExprProject;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.storm.executionengine.physicalLayer.expressionOperators.Subtract;
import org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.ScalarExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

public class LogicalExpressionCompiler extends LogicalExpressionVisitor {

  // This value points to the current LogicalRelationalOperator we are working
  // on
  protected LogicalRelationalOperator currentOp;

  public LogicalExpressionCompiler(OperatorPlan plan,
      LogicalRelationalOperator op, PhysicalPlan phyPlan,
      Map<Operator, PhysicalOperator> map) throws FrontendException {
    this(plan, new DependencyOrderWalker(plan), op, phyPlan, map);
  }

  public LogicalExpressionCompiler(OperatorPlan plan, PlanWalker walker,
      LogicalRelationalOperator op, PhysicalPlan phyPlan,
      Map<Operator, PhysicalOperator> map) throws FrontendException {
    super(plan, walker);
    currentOp = op;
    logToPhyMap = map;
    currentPlan = phyPlan;
    currentPlans = new LinkedList<PhysicalPlan>();
  }

  protected Map<Operator, PhysicalOperator> logToPhyMap;

  protected Deque<PhysicalPlan> currentPlans;

  protected PhysicalPlan currentPlan;

  protected NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();

  protected PigContext pc;

  public void setPigContext(PigContext pc) {
    this.pc = pc;
  }

  public PhysicalPlan getPhysicalPlan() {
    return currentPlan;
  }

  private void attachBinaryComparisonOperator(BinaryExpression op,
      BinaryComparisonOperator exprOp) throws FrontendException {
    // We dont have aliases in ExpressionOperators
    // exprOp.setAlias(op.getAlias());

    exprOp.setOperandType(op.getLhs().getType());
    exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhs()));
    exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhs()));
    OperatorPlan oPlan = op.getPlan();

    currentPlan.add(exprOp);
    logToPhyMap.put(op, exprOp);

    List<Operator> successors = oPlan.getSuccessors(op);
    if (successors == null) {
      return;
    }
    for (Operator lo : successors) {
      PhysicalOperator from = logToPhyMap.get(lo);
      try {
        currentPlan.connect(from, exprOp);
      } catch (PlanException e) {
        int errCode = 2015;
        String msg = "Invalid physical operators in the physical plan";
        throw new LogicalToPhysicalTranslatorException(msg, errCode,
            PigException.BUG, e);
      }
    }
  }

  private void attachBinaryExpressionOperator(BinaryExpression op,
      BinaryExpressionOperator exprOp) throws FrontendException {
    // We dont have aliases in ExpressionOperators
    // exprOp.setAlias(op.getAlias());

    exprOp.setResultType(op.getLhs().getType());
    exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhs()));
    exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhs()));
    OperatorPlan oPlan = op.getPlan();

    currentPlan.add(exprOp);
    logToPhyMap.put(op, exprOp);

    List<Operator> successors = oPlan.getSuccessors(op);
    if (successors == null) {
      return;
    }
    for (Operator lo : successors) {
      PhysicalOperator from = logToPhyMap.get(lo);
      try {
        currentPlan.connect(from, exprOp);
      } catch (PlanException e) {
        int errCode = 2015;
        String msg = "Invalid physical operators in the physical plan";
        throw new LogicalToPhysicalTranslatorException(msg, errCode,
            PigException.BUG, e);
      }
    }
  }

  @Override
  public void visit(AndExpression op) throws FrontendException {

    // System.err.println("Entering And");
    BinaryComparisonOperator exprOp = new POAnd(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryComparisonOperator(op, exprOp);
  }

  @Override
  public void visit(OrExpression op) throws FrontendException {

    // System.err.println("Entering Or");
    BinaryComparisonOperator exprOp = new POOr(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryComparisonOperator(op, exprOp);
  }

  @Override
  public void visit(EqualExpression op) throws FrontendException {

    BinaryComparisonOperator exprOp = new EqualToExpr(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryComparisonOperator(op, exprOp);
  }

  @Override
  public void visit(NotEqualExpression op) throws FrontendException {

    BinaryComparisonOperator exprOp = new NotEqualToExpr(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryComparisonOperator(op, exprOp);
  }

  @Override
  public void visit(GreaterThanExpression op) throws FrontendException {

    BinaryComparisonOperator exprOp = new GreaterThanExpr(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryComparisonOperator(op, exprOp);
  }

  @Override
  public void visit(GreaterThanEqualExpression op) throws FrontendException {

    BinaryComparisonOperator exprOp = new GTOrEqualToExpr(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryComparisonOperator(op, exprOp);
  }

  @Override
  public void visit(LessThanExpression op) throws FrontendException {

    BinaryComparisonOperator exprOp = new LessThanExpr(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryComparisonOperator(op, exprOp);
  }

  @Override
  public void visit(LessThanEqualExpression op) throws FrontendException {

    BinaryComparisonOperator exprOp = new LTOrEqualToExpr(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryComparisonOperator(op, exprOp);
  }

  @Override
  public void visit(ProjectExpression op) throws FrontendException {
    POProject exprOp;

    if (op.getAttachedRelationalOp() instanceof LOGenerate
        && op.getPlan().getSuccessors(op) == null
        && !(op.findReferent() instanceof LOInnerLoad)) {
      exprOp = new PORelationToExprProject(new OperatorKey(DEFAULT_SCOPE,
          nodeGen.getNextNodeId(DEFAULT_SCOPE)));
    } else {
      exprOp = new POProject(new OperatorKey(DEFAULT_SCOPE,
          nodeGen.getNextNodeId(DEFAULT_SCOPE)));
    }

    if (op.getFieldSchema() == null && op.isRangeOrStarProject())
      exprOp.setResultType(DataType.TUPLE);
    else
      exprOp.setResultType(op.getType());
    if (op.isProjectStar()) {
      exprOp.setStar(op.isProjectStar());
    } else if (op.isRangeProject()) {
      if (op.getEndCol() != -1) {
        // all other project-range should have been expanded by
        // project-star expander
        throw new AssertionError("project range that is not a "
            + "project-to-end seen in translation to physical plan!");
      }
      exprOp.setProjectToEnd(op.getStartCol());
    } else {
      exprOp.setColumn(op.getColNum());
    }
    // TODO implement this
    // exprOp.setOverloaded(op.getOverloaded());
    logToPhyMap.put(op, exprOp);
    currentPlan.add(exprOp);
  }

  @Override
  public void visit(MapLookupExpression op) throws FrontendException {
    ExpressionOperator physOp = new POMapLookUp(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));
    ((POMapLookUp) physOp).setLookUpKey(op.getLookupKey());
    physOp.setResultType(op.getType());
    physOp.addOriginalLocation(op.getFieldSchema().alias, op.getLocation());
    currentPlan.add(physOp);

    logToPhyMap.put(op, physOp);

    ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op.getMap());
    try {
      currentPlan.connect(from, physOp);
    } catch (PlanException e) {
      int errCode = 2015;
      String msg = "Invalid physical operators in the physical plan";
      throw new LogicalToPhysicalTranslatorException(msg, errCode,
          PigException.BUG, e);
    }
  }

  @Override
  public void visit(
      org.apache.pig.newplan.logical.expression.ConstantExpression op)
      throws FrontendException {

    // System.err.println("Entering Constant");
    ConstantExpression ce = new ConstantExpression(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
    // We dont have aliases in ExpressionOperators
    // ce.setAlias(op.getAlias());
    ce.setValue(op.getValue());
    ce.setResultType(op.getType());
    // this operator doesn't have any predecessors
    currentPlan.add(ce);
    logToPhyMap.put(op, ce);
    // System.err.println("Exiting Constant");
  }

  @Override
  public void visit(CastExpression op) throws FrontendException {
    POCast pCast = new POCast(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));
    // physOp.setAlias(op.getAlias());
    currentPlan.add(pCast);

    logToPhyMap.put(op, pCast);
    ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
        .getExpression());
    pCast.setResultType(op.getType());
    pCast.setFieldSchema(new ResourceSchema.ResourceFieldSchema(op
        .getFieldSchema()));
    FuncSpec lfSpec = op.getFuncSpec();
    if (null != lfSpec) {
      try {
        pCast.setFuncSpec(lfSpec);
      } catch (IOException e) {
        int errCode = 1053;
        String msg = "Cannot resolve load function to use for casting"
            + " from " + DataType.findTypeName(op.getExpression().getType())
            + " to " + DataType.findTypeName(op.getType());
        throw new LogicalToPhysicalTranslatorException(msg, errCode,
            PigException.BUG, e);
      }
    }
    try {
      currentPlan.connect(from, pCast);
    } catch (PlanException e) {
      int errCode = 2015;
      String msg = "Invalid physical operators in the physical plan";
      throw new LogicalToPhysicalTranslatorException(msg, errCode,
          PigException.BUG, e);
    }
  }

  @Override
  public void visit(NotExpression op) throws FrontendException {

    PONot pNot = new PONot(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));
    // physOp.setAlias(op.getAlias());
    currentPlan.add(pNot);

    logToPhyMap.put(op, pNot);
    ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
        .getExpression());
    pNot.setExpr(from);
    pNot.setResultType(op.getType());
    pNot.setOperandType(op.getType());
    try {
      currentPlan.connect(from, pNot);
    } catch (PlanException e) {
      int errCode = 2015;
      String msg = "Invalid physical operators in the physical plan";
      throw new LogicalToPhysicalTranslatorException(msg, errCode,
          PigException.BUG, e);
    }
  }

  @Override
  public void visit(IsNullExpression op) throws FrontendException {
    POIsNull pIsNull = new POIsNull(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));
    // physOp.setAlias(op.getAlias());
    currentPlan.add(pIsNull);

    logToPhyMap.put(op, pIsNull);
    ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
        .getExpression());
    pIsNull.setExpr(from);
    pIsNull.setResultType(op.getType());
    pIsNull.setOperandType(op.getExpression().getType());
    try {
      currentPlan.connect(from, pIsNull);
    } catch (PlanException e) {
      int errCode = 2015;
      String msg = "Invalid physical operators in the physical plan";
      throw new LogicalToPhysicalTranslatorException(msg, errCode,
          PigException.BUG, e);
    }
  }

  @Override
  public void visit(NegativeExpression op) throws FrontendException {
    PONegative pNegative = new PONegative(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));
    // physOp.setAlias(op.getAlias());
    currentPlan.add(pNegative);

    logToPhyMap.put(op, pNegative);
    ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
        .getExpression());
    pNegative.setExpr(from);
    pNegative.setResultType(op.getType());
    try {
      currentPlan.connect(from, pNegative);
    } catch (PlanException e) {
      int errCode = 2015;
      String msg = "Invalid physical operators in the physical plan";
      throw new LogicalToPhysicalTranslatorException(msg, errCode,
          PigException.BUG, e);
    }
  }

  @Override
  public void visit(AddExpression op) throws FrontendException {
    BinaryExpressionOperator exprOp = new Add(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryExpressionOperator(op, exprOp);
  }

  @Override
  public void visit(RegexExpression op) throws FrontendException {
    BinaryExpressionOperator exprOp = new PORegexp(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryExpressionOperator(op, exprOp);

    List<Operator> successors = op.getPlan().getSuccessors(op);
    if (successors.get(1) instanceof org.apache.pig.newplan.logical.expression.ConstantExpression) {
      ((PORegexp) exprOp).setConstExpr(true);
    }
  }

  @Override
  public void visit(SubtractExpression op) throws FrontendException {
    BinaryExpressionOperator exprOp = new Subtract(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryExpressionOperator(op, exprOp);
  }

  @Override
  public void visit(MultiplyExpression op) throws FrontendException {
    BinaryExpressionOperator exprOp = new Multiply(new OperatorKey(
        DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryExpressionOperator(op, exprOp);
  }

  @Override
  public void visit(DivideExpression op) throws FrontendException {
    BinaryExpressionOperator exprOp = new Divide(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryExpressionOperator(op, exprOp);
  }

  @Override
  public void visit(ModExpression op) throws FrontendException {
    BinaryExpressionOperator exprOp = new Mod(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    attachBinaryExpressionOperator(op, exprOp);
  }

  @Override
  public void visit(BinCondExpression op) throws FrontendException {

    POBinCond exprOp = new POBinCond(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    exprOp.setResultType(op.getType());
    exprOp.setCond((ExpressionOperator) logToPhyMap.get(op.getCondition()));
    exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhs()));
    exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhs()));
    OperatorPlan oPlan = op.getPlan();

    currentPlan.add(exprOp);
    logToPhyMap.put(op, exprOp);

    List<Operator> successors = oPlan.getSuccessors(op);
    if (successors == null) {
      return;
    }
    for (Operator lo : successors) {
      PhysicalOperator from = logToPhyMap.get(lo);
      try {
        currentPlan.connect(from, exprOp);
      } catch (PlanException e) {
        int errCode = 2015;
        String msg = "Invalid physical operators in the physical plan";
        throw new LogicalToPhysicalTranslatorException(msg, errCode,
            PigException.BUG, e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void visit(UserFuncExpression op) throws FrontendException {
    Object f = PigContext.instantiateFuncFromSpec(op.getFuncSpec());
    PhysicalOperator p;
    String outerOperatorAlias = this.currentOp.getAlias();
    if (f instanceof EvalFunc) {
      p = new POUserFunc(new OperatorKey(DEFAULT_SCOPE,
          nodeGen.getNextNodeId(DEFAULT_SCOPE)), -1, null, op.getFuncSpec(),
          (EvalFunc) f);
      ((POUserFunc) p).setWorkingOnOperatorAlias(outerOperatorAlias);
      ((POUserFunc) p).setSignature(op.getSignature());
      List<String> cacheFiles = ((EvalFunc) f).getCacheFiles();
      if (cacheFiles != null) {
        ((POUserFunc) p).setCacheFiles(cacheFiles.toArray(new String[cacheFiles
            .size()]));
      }
    } else {
      p = new POUserComparisonFunc(new OperatorKey(DEFAULT_SCOPE,
          nodeGen.getNextNodeId(DEFAULT_SCOPE)), -1, null, op.getFuncSpec(),
          (ComparisonFunc) f);
      ((POUserFunc) p).setWorkingOnOperatorAlias(outerOperatorAlias);
    }
    p.setResultType(op.getType());
    currentPlan.add(p);
    List<LogicalExpression> fromList = op.getArguments();
    if (fromList != null) {
      for (LogicalExpression inputOperator : fromList) {
        PhysicalOperator from = logToPhyMap.get(inputOperator);
        try {
          currentPlan.connect(from, p);
        } catch (PlanException e) {
          int errCode = 2015;
          String msg = "Invalid physical operators in the physical plan";
          throw new LogicalToPhysicalTranslatorException(msg, errCode,
              PigException.BUG, e);
        }
      }
    }
    logToPhyMap.put(op, p);

    // We need to track all the scalars
    if (op instanceof ScalarExpression) {
      Operator refOp = ((ScalarExpression) op).getImplicitReferencedOperator();
      ((POUserFunc) p).setReferencedOperator(logToPhyMap.get(refOp));
    }

  }

  @Override
  public void visit(DereferenceExpression op) throws FrontendException {
    POProject exprOp = new POProject(new OperatorKey(DEFAULT_SCOPE,
        nodeGen.getNextNodeId(DEFAULT_SCOPE)));

    exprOp.setResultType(op.getType());
    exprOp.setColumns((ArrayList<Integer>) op.getBagColumns());
    exprOp.setStar(false);
    logToPhyMap.put(op, exprOp);
    currentPlan.add(exprOp);

    PhysicalOperator from = logToPhyMap.get(op.getReferredExpression());

    if (from != null) {
      currentPlan.connect(from, exprOp);
    }
  }
}
