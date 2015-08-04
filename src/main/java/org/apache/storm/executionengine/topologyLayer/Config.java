package org.apache.storm.executionengine.topologyLayer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class Config implements Serializable {
  public static final String UDF_CONF_KEY = "KEY_POS_UDF_CONF";
  public static final String RELALTION_CONF_KEY = "KEY_Relation_CONF";
  private static final long serialVersionUID = 1L;
  private HashMap yamlConf = null;

  public Config(Map yamlConf, Class funcClass, String relationAlias) {
    this(yamlConf, funcClass == null ? null : funcClass.toString(),
        relationAlias);
  }

  public Config(Map yamlConf, String funcClass, String relationAlias) {
    this.yamlConf = new HashMap();
    if (yamlConf == null) {
      return;
    }

    this.yamlConf.putAll(yamlConf);

    Map allUDFConf = (Map) this.yamlConf.remove(UDF_CONF_KEY);

    if (allUDFConf == null || funcClass == null) {
      return;
    }

    Map funcConf = (Map) allUDFConf.get(funcClass);

    if (funcConf == null) {
      return;
    }

    funcConf = shallowCopy(funcConf);
    Map allRelationConf = (Map) funcConf.remove(RELALTION_CONF_KEY);
    this.yamlConf.putAll(funcConf);

    if (allRelationConf == null || relationAlias == null) {
      return;
    }

    Map relationConf = (Map) allRelationConf.get(relationAlias);
    if (relationConf != null) {
      this.yamlConf.putAll(relationConf);
    }
  }

  private Map shallowCopy(Map map) {
    Map m = new HashMap();
    m.putAll(map);
    return m;
  }

  public Object getValue(Object key) {
    return this.yamlConf.get(key);
  }

  public Object getIntValue(Object key) {
    return (Integer) this.yamlConf.get(key);
  }

  public String getStringValue(Object key) {
    return (String) this.yamlConf.get(key);
  }

  public Map getMapValue(Object key) {
    return (Map) this.yamlConf.get(key);
  }

  public List getListValue(Object key) {
    return (List) this.yamlConf.get(key);
  }

  public String getStringValue(Object key, String defaultValue) {
    String value = this.getStringValue(key);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  public Map<String, String> asStringMap() {
    Map<String, String> map = new HashMap<String, String>(this.yamlConf.size());
    Iterator it = this.yamlConf.keySet().iterator();
    Object key = null;
    Object value = null;
    while (it.hasNext()) {
      key = it.next();
      value = this.yamlConf.get(key);
      map.put(key.toString(), value == null ? null : value.toString());
    }

    return map;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((yamlConf == null) ? 0 : yamlConf.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Config other = (Config) obj;
    if (yamlConf == null) {
      if (other.yamlConf != null)
        return false;
    } else if (!yamlConf.equals(other.yamlConf))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Config [yamlConf=" + yamlConf + "]";
  }

  public static Config loadConfig(String yamlPath) throws FileNotFoundException {
    Yaml yaml = new Yaml();
    Map yamlMap = (Map) yaml.load(new InputStreamReader(new FileInputStream(
        yamlPath)));
    return new Config(yamlMap, (String) null, null);
  }

  public static Config loadConfigByResourePath(Class clazz, String resourcePath) {
    Yaml yaml = new Yaml();
    Map yamlMap = (Map) yaml.load(new InputStreamReader(clazz
        .getResourceAsStream(resourcePath)));
    return new Config(yamlMap, (String) null, null);
  }
}
