package org.apache.storm.monitor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Utils;

public abstract class BaseMonitor implements IMonitor {

	private boolean disableAll = true;
	private Set<String> disableSysSet = new HashSet<String>();
	private Set<String> disableIntfSet = new HashSet<String>();

	@Override
	public void init(Map conf) {
		disableAll = Utils.getBoolean(conf, "monitor.all.disable", false);
		String sysIDs = Utils.get(conf, "monitor.disable.sysids", "");

		if (sysIDs.length() != 0) {
			for (String sysID : sysIDs.split(",")) {
				disableSysSet.add(sysID);
			}
		}

		String intfs = Utils.get(conf, "monitor.disable.intfids", "");
		if (sysIDs.length() != 0) {
			for (String IntfID : intfs.split(",")) {
				disableIntfSet.add(IntfID);
			}
		}

	}

	protected boolean check(String sysID, String intfID) {
		if (disableAll) {
			return false;
		}
		if (disableSysSet.contains(sysID)) {
			return false;
		}
		if (disableIntfSet.contains(intfID)) {
			return false;
		}
		return true;
	}

}
