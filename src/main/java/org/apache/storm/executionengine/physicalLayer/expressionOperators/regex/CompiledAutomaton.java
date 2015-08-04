package org.apache.storm.executionengine.physicalLayer.expressionOperators.regex;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

public class CompiledAutomaton implements RegexImpl {

  private RunAutomaton runauto = null;

  public CompiledAutomaton(String rhsPattern) {
    RegExp regexpr = new dk.brics.automaton.RegExp(rhsPattern, RegExp.NONE);
    Automaton auto = regexpr.toAutomaton();
    this.runauto = new RunAutomaton(auto, true);
  }

  @Override
  public boolean match(String lhs, String rhs) {
    return this.runauto.run(lhs);
  }

}
