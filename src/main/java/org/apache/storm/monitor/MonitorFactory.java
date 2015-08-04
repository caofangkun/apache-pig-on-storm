package org.apache.storm.monitor;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.storm.Utils;
import org.apache.storm.executionengine.topologyLayer.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorFactory {
	private static final Logger logger = LoggerFactory
			.getLogger(MonitorFactory.class);

	private MonitorFactory() {

	}

	public static IMonitor newMonitor(Map config) throws ExecException {
		String clazzName = Utils.get(config,"class.monitor.impl",
				"org.apache.storm.monitor.DefaultMonitorImpl");
		if (clazzName != null) {
			try {
				Class clazz = Class.forName(clazzName);
				IMonitor im = (IMonitor) clazz.newInstance();
				im.init(config);
				logger.info("class.monitor.impl={}", clazzName);
				return im;
			} catch (Exception e) {
				logger.error("fail to newMonitor,and clazzName={}", clazzName);
			}
		}

		return null;
	}
	
	public static void main(String[] args) throws ExecException
	{
//		Map cm = new HashMap();
//		Config conf = new Config(cm,"",null);
//		IMonitor im = MonitorFactory.newMonitor(conf);
//		im.reportCount("sysID", "intID", 0, 0, new HashMap(), 1);
	}
}
