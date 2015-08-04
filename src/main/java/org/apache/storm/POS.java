package org.apache.storm;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class POS {
  public static final String OnYarn = "onyarn";
  public static final String CONFIG = "config";
  public static final String SCRIPT_PATH = "script";
  public static final String SEPARATOR = "=";
  private static Map<String, String> optMap = new HashMap<String, String>();
  private static Map jobConfig = null;
  private static String script = null;

  public static void main(String[] args) throws Exception {
    optMap.put(CONFIG, null);
    if (args != null && args.length > 0) {
      for (String opt : args) {
        String[] kv = opt.split(SEPARATOR);
        if (kv.length != 2) {
          throw new IllegalArgumentException(
              "Expected option style is 'key=value', actully is '" + opt + "'");
        }
        optMap.put(kv[0].trim(), kv[1].trim());
      }
    }

    boolean onYarn = false;

    Iterator<String> keys = optMap.keySet().iterator();
    while (keys.hasNext()) {
      String key = keys.next();
      if (CONFIG.equalsIgnoreCase(key)) {
        handleConfig(optMap.get(key));
      } else if (SCRIPT_PATH.equalsIgnoreCase(key)) {
        handleScript(optMap.get(key));
      } else if (OnYarn.equalsIgnoreCase(key)) {
        onYarn = true;
      } else {
        System.out.println("Unrecognized option, Usage is:");
        printUsage();
        return;
      }
    }

    // TODO:use embedded script
    if (script == null || script.equals("")) {
      extractEmbeddedScript();
    }
    if (onYarn) {
      launchStormOnYarn(jobConfig, script);
    } else {
      launchStorm(jobConfig, script);
    }
  }

  private static void printUsage() {
    System.out
        .println("storm jar org.apache.storm.POS script=/opt/storm/script/ctr.pig [config=/opt/storm/conf/job.yaml]");
  }

  private static void launchStorm(Map config, String script) throws Exception {
    Utils.launchStorm(config, script);
  }

  private static void launchStormOnYarn(Map config, String script)
      throws Exception {
    Utils.launchStormOnYarn(config, script);
  }

  private static void handleConfig(String configPath)
      throws FileNotFoundException {
    Yaml yaml = new Yaml();
    jobConfig = (Map) yaml.load(new InputStreamReader(new FileInputStream(
        configPath)));
  }

  private static void handleScript(String scriptPath) throws Exception {
    List<String> scriptCache = new ArrayList<String>();
    BufferedReader reader = new BufferedReader(new FileReader(scriptPath));
    while (true) {
      String line = reader.readLine();
      if (line != null) {
        scriptCache.add(line);
      } else {
        break;
      }
    }

    StringBuilder accuQuery = new StringBuilder();
    for (String line : scriptCache) {
      accuQuery.append(line + "\n");
    }
    reader.close();
    scriptCache.clear();
    script = accuQuery.toString();

    if (script == null || script.length() == 0) {
      throw new Exception("Script content is empty. Path: " + scriptPath);
    }
  }

  private static void extractEmbeddedScript() throws Exception {
    InputStreamReader isr = new InputStreamReader(
        POS.class.getResourceAsStream("/pos/pos.pig"));
    /**
     * 
     */
    List<String> scriptCache = new ArrayList<String>();
    BufferedReader reader = new BufferedReader(isr);
    while (true) {
      String line = reader.readLine();
      if (line != null) {
        scriptCache.add(line);
      } else {
        break;
      }
    }
    //
    StringBuilder accuQuery = new StringBuilder();
    for (String line : scriptCache) {
      accuQuery.append(line + "\n");
    }
    reader.close();
    scriptCache.clear();
    script = accuQuery.toString();
    /**
     * 
     */
    if (script == null || script.length() == 0) {
      throw new Exception("Script content is empty. ClassPath: /pos/pos.pig");
    }
  }
}
