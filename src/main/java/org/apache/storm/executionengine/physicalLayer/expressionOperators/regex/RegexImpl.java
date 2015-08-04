package org.apache.storm.executionengine.physicalLayer.expressionOperators.regex;

// General interface for regexComparison
public interface RegexImpl {
  boolean match(String lhs, String rhs);
}
