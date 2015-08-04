package org.apache.storm.executionengine.physicalLayer.expressionOperators.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompiledRegex implements RegexImpl {

  private Matcher m = null;

  public CompiledRegex(Pattern rhsPattern) {
    this.m = rhsPattern.matcher("");
  }

  @Override
  public boolean match(String lhs, String rhs) {
    m.reset(lhs);
    return m.matches();
  }

}
