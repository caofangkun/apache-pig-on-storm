package org.apache.storm.monitor.impl;

import java.util.Map;

import org.apache.storm.Utils;
import org.apache.storm.monitor.BaseMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.tencent.monitor.MonitorEntry;
import com.tencent.monitor.MonitorTools;

/**
 * 返回类型 public static final int SUCCESSCODE = 0; public static final int
 * BUSSINESS_ERRORCODE = 2; public static final int SYSTEM_ERRORCODE = 3;
 * 
 * @author gavinxie
 * 
 */
public class RealTimeMonitorImpl extends BaseMonitor {
	private MonitorTools mt;
	private static int type_count = 1;
	private static int type_latency = 2;

	// 返回类型定义,成功
	public static int SUCCESSCODE = 0;
	// 业务失败
	public static int BUSSINESS_ERRORCODE = 2;
	// 系统失败
	public static int SYSTEM_ERRORCODE = 3;

	// 系统缺省错误码定义
	public static int ERROR_CODE_SYSTEM_DEFAULT = -1;

	private static final Logger logger = LoggerFactory
			.getLogger(RealTimeMonitorImpl.class);

	private boolean debug = false;

	public RealTimeMonitorImpl() {

	}

	@Override
	public void init(Map conf) {
		super.init(conf);
		mt = MonitorTools.getMonitorInstance(conf);
		debug = Utils.getBoolean(conf, "debug", false);

	}

	@Override
	public void reportCount(String sysID, String intID, int returnType,
			int errcode, Map<String, Object> dims, int count) {
		this.report(sysID, intID, returnType, errcode, dims, count, type_count);
	}

	@Override
	public void reportLatency(String sysID, String intID, int returnType,
			int errcode, Map<String, Object> dims, int latency) {
		this.report(sysID, intID, returnType, errcode, dims, latency,
				type_latency);

	}

	private void report(String sysID, String intID, int returnType,
			int errcode, Map<String, Object> dims, int value, int indexType) {
		if (!check(sysID, intID)) {
			return;
		}
		if (null == sysID || null == intID) {
			logger.error("RealTimeMonitor,report: null == sysID || null == intID");
			return;
		}
		if (0 == sysID.length() || 0 == intID.length()) {
			logger.error("RealTimeMonitor,report: 0 == sysID.length() || 0 == intID.length()");
			return;
		}

		String dimArray[] = null;
		if (dims != null) {
			dimArray = new String[dims.size() * 2];
			int i = 0;
			for (Map.Entry<String, Object> entry : dims.entrySet()) {
				dimArray[i] = entry.getKey();
				dimArray[i + 1] = String.valueOf(entry.getValue());
				i += 2;
			}
		} else {
			dimArray = new String[0];
		}
		if (debug) {
			logger.info(
					"success to send to realmonitor,and sysID={},intID={},returnType={},errcode={},dimArray={},value={}",
					new Object[] { sysID, intID, returnType, errcode, dimArray,
							value });
		}
		if (indexType == type_count) {
			mt.addCountEntry(sysID, intID, new MonitorEntry(returnType,
					errcode, dimArray), value);

		} else if (indexType == type_latency) {
			mt.addLatencyEntry(sysID, intID, new MonitorEntry(returnType,
					errcode, dimArray), value);
		} else {
			logger.error("unkown type,and indexType={}", indexType);
		}
	}

}
