package org.apache.storm.executionengine.physicalLayer.expressionOperators.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NonConstantRegex implements RegexImpl {

  private Pattern pattern = null;

  private String oldString = null;

  private Matcher matcher = null;

  @Override
  public boolean match(String lhs, String rhs) {
    // We first check for length so the comparison is faster
    // and then we directly check for difference.
    // I havent used equals as first two comparisons,
    // same Object and isInstanceOf does not apply in this case.
    if (oldString == null || rhs.length() != oldString.length()
        || rhs.compareTo(oldString) != 0) {
      oldString = rhs;
      pattern = Pattern.compile(oldString);
      matcher = pattern.matcher(lhs);
    } else {
      matcher.reset(lhs);
    }
    return matcher.matches();
  }

}
