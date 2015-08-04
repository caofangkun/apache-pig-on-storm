package org.apache.storm.monitor.impl;

import java.util.Map;

import org.apache.storm.monitor.BaseMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMonitorImpl extends BaseMonitor {

	private static final Logger logger = LoggerFactory
			.getLogger(DefaultMonitorImpl.class);

	@Override
	public void init(Map conf) {
		logger.info("DefaultMonitor:init, conf={}", conf.toString());
	}

	@Override
	public void reportCount(String sysID, String intID, int returnType,
			int errcode, Map<String, Object> dims, int count) {
		logger.info(
				"DefaultMonitor:reportCount, sysID={},intID={},returnType={},errcode={},dims={},count={}",
				new Object[] { sysID, intID, returnType, errcode,
						dims.toString(), count });

	}

	@Override
	public void reportLatency(String sysID, String intID, int returnType,
			int errcode, Map<String, Object> dims, int latency) {
		logger.info(
				"reportLatency, sysID={},intID={},returnType={},errcode={},dims={},latency={}",
				new Object[] { sysID, intID, returnType, errcode,
						dims.toString(), latency });
	}

}
