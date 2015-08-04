package org.apache.storm.monitor;

import java.util.Map;

public interface IMonitor {
	
	void init(Map conf) ;

	/**
	 * 
	 * @param sysID
	 * @param intID
	 * @param returnType
	 *            成功/业务/系统
	 * @param errcode
	 * @param dims
	 * @param count
	 */
	void reportCount(String sysID, String intID, int returnType, int errcode,
			Map<String,Object> dims, int count);

	/**
	 * 
	 * @param sysID
	 * @param intID
	 * @param returnType
	 *            成功/业务/系统
	 * @param errcode
	 * @param dims
	 * @param latency
	 */
	void reportLatency(String sysID, String intID, int returnType, int errcode,
			Map<String,Object> dims, int latency);
}
